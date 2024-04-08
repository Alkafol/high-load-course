package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import ru.quipy.common.utils.CustomPolicy
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.AccountProperties
import ru.quipy.payments.config.AccountStatisticsService
import ru.quipy.payments.config.ExternalServicesConfig
import java.time.Duration
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

// Advice: always treat time as a Duration
@Component
class PaymentExternalServiceImpl @Autowired constructor(
    private val accountStatisticsService: AccountStatisticsService,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val requestExecutor: RequestExecutor,
    private val window: NonBlockingOngoingWindow,
) : PaymentExternalService {

    private val baseProperties = ExternalServicesConfig.PRIMARY_ACCOUNT

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private val limiterQueue: BlockingQueue<Runnable> = ArrayBlockingQueue(1000)
        private val windowQueue: BlockingQueue<Runnable> = ArrayBlockingQueue(1000)

        private val windowPool = ThreadPoolExecutor(1, 3, 10, TimeUnit.SECONDS, windowQueue, CustomPolicy())
        private val limiterPool = ThreadPoolExecutor(1, 3, 10, TimeUnit.SECONDS, limiterQueue, CustomPolicy())
    }

    private val serviceName = baseProperties.serviceName
    private val accountName = baseProperties.accountName
    private val requestAverageProcessingTime = baseProperties.request95thPercentileProcessingTime
    private val rateLimitPerSec = baseProperties.rateLimitPerSec
    private val parallelRequests = baseProperties.parallelRequests

    private val requestSenderPool = Executors.newFixedThreadPool(100)
    private var predictedExtAccount = baseProperties
    private var predictedAccount: AtomicReference<AccountProperties?> = AtomicReference(accountStatisticsService.getProperties(predictedExtAccount))
    private val rateLimiter = RateLimiter(600)

    init {
        accountStatisticsService.statistics.entries.forEach {
            it.value.pool.prestartAllCoreThreads()
        }
        windowPool.prestartAllCoreThreads()
        limiterPool.prestartAllCoreThreads()
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

        // rate limiter -- 600 per ser
        if (!limiterQueue.offer(Runnable {
            while (true) {
                if (rateLimiter.tick()) {
                    if (!windowQueue.offer(Runnable {
                        while (true) {
                            if (window.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Success) {
                             //   Thread.sleep(5000)
                                val requestData = RequestData(paymentId, amount, paymentStartedAt, transactionId)
                                predictAccountAndSend(requestData)

                                return@Runnable
                            }
                        }
                    })) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Window overfill")
                        }
                    }
                    return@Runnable
                }
            }
        })) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limiter queue overfill")
            }
        }
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
                if (!targetAccount.queue.offer(Runnable {
                    while (!targetAccount.limiter.tick()) {
                        continue
                    }
                    requestExecutor.sendRequest(requestData, targetAccount)
                })) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Account queue overfill")
                    }
                }

                val cheaperAccount = ExternalServicesConfig.getCheaper(targetAccount.extProperties)
                if (cheaperAccount != null) {
                    predictedAccount.compareAndSet(targetAccount, accountStatisticsService.getProperties(cheaperAccount))
                }

                break
            }
        }
    }
}

public fun now() = System.currentTimeMillis()