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
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference
import kotlin.Comparator


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
    private val window = NonBlockingOngoingWindow(500)
    private val rateLimiter = RateLimiter(100)

    init {
        accountStatisticsService.statistics.entries.forEach {
            requestExecutor.execute(it.value)
        }
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

        // rate limiter -- 100 per ser
        if (!rateLimiter.tick()) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limiter overfill")
            }

            return
        }

        // window -- 500 in parallel
        if (window.putIntoWindow() == NonBlockingOngoingWindow.WindowResponse.Fail(100)){
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Window overfill")
            }

            return
        }

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
                val requestData = RequestData(paymentId, amount, paymentStartedAt, transactionId)
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
}

public fun now() = System.currentTimeMillis()