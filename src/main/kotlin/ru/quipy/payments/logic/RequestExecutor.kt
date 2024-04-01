package ru.quipy.payments.logic

import okhttp3.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.AccountProperties
import ru.quipy.payments.config.AccountStatisticsService
import java.net.SocketTimeoutException
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

    private val httpClientExecutor = Executors.newFixedThreadPool(20)
    private val client = OkHttpClient.Builder()
        .followRedirects(false)
        .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
        .connectionPool(ConnectionPool(100, 1000, TimeUnit.MILLISECONDS))
        .run {
            dispatcher(Dispatcher(httpClientExecutor))
            build()
        }
    private val responsePool = Executors.newFixedThreadPool(10)

    fun execute(accountProperties: AccountProperties) {
        Thread {
            while (true) {
                val curRequestData = accountProperties.queue.poll()
                if (curRequestData != null) {
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

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=${transactionId}")
            post(PaymentExternalServiceImpl.emptyBody)
        }.build()

        // TODO: use submit + separate threads
        try {
            client.newCall(request).execute().use { response ->
                val body = try {
                    //    val parsedBody = response.body?.string()
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

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    accountStatisticsService.receiveResponse(accountProperties.extProperties)
                    window.releaseWindow()
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    PaymentExternalServiceImpl.logger.error(
                        "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                        e
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }

            accountStatisticsService.receiveResponse(accountProperties.extProperties)
            window.releaseWindow()
        }
    }
}