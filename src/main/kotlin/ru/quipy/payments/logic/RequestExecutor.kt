package ru.quipy.payments.logic

import okhttp3.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.AccountProperties
import ru.quipy.payments.config.AccountStatisticsService
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.time.Instant
import java.util.Collections
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

data class RequestData(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val transactionId: UUID
)

@Component
class RequestExecutor @Autowired constructor(
    private val accountStatisticsService: AccountStatisticsService,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val window: NonBlockingOngoingWindow
) {

    private val requestTimeout: Duration = Duration.ofMillis(80000)
    private val httpClientExecutor = Executors.newFixedThreadPool(100)
    private val client = OkHttpClient.Builder()
       // .followRedirects(false)
        .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
    //    .connectionPool(ConnectionPool(1000, 10000, TimeUnit.MILLISECONDS))
        .run {
            dispatcher(
                Dispatcher(httpClientExecutor).apply {
                    maxRequestsPerHost = 50
                    maxRequests = 50
                }
            )
            build()
        }
    private val responsePool = Executors.newFixedThreadPool(10)

    fun execute(accountProperties: AccountProperties) {
        val limiter = RateLimiter(accountProperties.extProperties.rateLimitPerSec)
        Thread {
            while (true) {
                val curRequestData = accountProperties.queue.poll()
                if (curRequestData != null) {
                    while (!limiter.tick()) {
                        continue
                    }
                    accountProperties.pool.execute {
                        sendRequest(curRequestData, accountProperties)
                    }
                }
            }
        }.start()
    }

    private fun sendRequest(requestData: RequestData, accountProperties: AccountProperties) {
        val serviceName = accountProperties.extProperties.serviceName
        val accountName = accountProperties.extProperties.accountName
        val transactionId = requestData.transactionId
        val paymentId = requestData.paymentId

        val timeSinceRequestCreation = Duration.ofMillis(now() - requestData.paymentStartedAt)
        if (timeSinceRequestCreation > requestTimeout) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request is out of time")
            }
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=${transactionId}")
            post(PaymentExternalServiceImpl.emptyBody)
        }.build()

        client.newCall(request)
                .enqueue(
                        object : Callback {
                            override fun onResponse(call: Call, response: Response) {
                                response.use {
                                    val body = try {
                                        PaymentExternalServiceImpl.mapper.readValue(
                                            response.body?.string(),
                                            ExternalSysResponse::class.java
                                        )
                                    } catch (e: Exception) {
                                        PaymentExternalServiceImpl.logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                                        ExternalSysResponse(false, e.message)
                                    }

                                    responsePool.execute {
                                        PaymentExternalServiceImpl.logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                                        paymentESService.update(paymentId) {
                                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                                        }
                                    }

                                    accountStatisticsService.receiveResponse(accountProperties.extProperties)
                                    window.releaseWindow()
                                }
                            }

                            override fun onFailure(call: Call, e: IOException) {
                                PaymentExternalServiceImpl.logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, reason: ${e.message}")

                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = e.message)
                                }

                                accountStatisticsService.receiveResponse(accountProperties.extProperties)
                                window.releaseWindow()
                            }
                        }
                )
    }
}