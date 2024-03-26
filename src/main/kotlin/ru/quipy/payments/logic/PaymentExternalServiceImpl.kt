package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.AccountProperties
import ru.quipy.payments.config.AccountStatisticsService
import ru.quipy.payments.config.ExternalServicesConfig
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference

// Advice: always treat time as a Duration
@Component
class PaymentExternalServiceImpl @Autowired constructor(
    private val accountStatisticsService: AccountStatisticsService,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val requestExecutor: RequestExecutor
) : PaymentExternalService {

    private val baseProperties = ExternalServicesConfig.PRIMARY_ACCOUNT

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = baseProperties.serviceName
    private val accountName = baseProperties.accountName
    private val requestAverageProcessingTime = baseProperties.request95thPercentileProcessingTime
    private val rateLimitPerSec = baseProperties.rateLimitPerSec
    private val parallelRequests = baseProperties.parallelRequests

    private val requestSenderPool = Executors.newFixedThreadPool(100)
    private var predictedExtAccount = baseProperties
    private var predictedAccount: AtomicReference<AccountProperties?> = AtomicReference(accountStatisticsService.getProperties(predictedExtAccount))
    private val window = NonBlockingOngoingWindow(2000)
    private val rateLimiter = RateLimiter(600)

    private val limiterQueue: ConcurrentLinkedQueue<RequestData> = ConcurrentLinkedQueue()
    private val windowQueue: ConcurrentLinkedQueue<RequestData> = ConcurrentLinkedQueue()

    init {
        accountStatisticsService.statistics.entries.forEach {
            requestExecutor.execute(it.value)
        }

        runLimiterWorker()
        runWindowWorker()
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val requestData = RequestData(paymentId, amount, paymentStartedAt, transactionId)

        // rate limiter -- 600 per ser
        limiterQueue.add(requestData)
    }

    private fun predictAccountAndSend(requestData: RequestData) {
        val paymentId = requestData.paymentId
        val transactionId = requestData.transactionId

        while (true) {
            val targetAccount = predictedAccount.get() ?: error("Predicted account can't be null")
            if (!accountStatisticsService.trySendRequest(targetAccount.extProperties)) {
                val costlierAccount = ExternalServicesConfig.getCostlier(targetAccount.extProperties)

                if (costlierAccount == null) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "No available accounts")
                    }
                    break
                }

                predictedAccount.compareAndSet(targetAccount, accountStatisticsService.getProperties(costlierAccount))
            } else {
                targetAccount.queue.add(requestData)

                val cheaperAccount = ExternalServicesConfig.getCheaper(targetAccount.extProperties)
                if (cheaperAccount != null) {
                    predictedAccount.compareAndSet(targetAccount, accountStatisticsService.getProperties(cheaperAccount))
                }

                break
            }
        }

        window.releaseWindow()
    }

    private fun runWindowWorker() {
        Thread {
            while (true) {
                if (windowQueue.isNotEmpty()) {
                    if (window.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Success) {
                        predictAccountAndSend(windowQueue.poll())
                    }
                }
            }
        }.start()
    }

    private fun runLimiterWorker() {
        Thread {
            while (true) {
                if (limiterQueue.isNotEmpty()) {
                    if (rateLimiter.tick()) {
                        windowQueue.add(limiterQueue.poll())
                    }
                }
            }
        }.start()
    }
}

public fun now() = System.currentTimeMillis()