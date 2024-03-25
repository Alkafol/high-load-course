package ru.quipy.payments.logic

import okhttp3.OkHttpClient
import okhttp3.Request
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.AccountProperties
import java.net.SocketTimeoutException
import java.util.UUID

data class RequestData(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val transactionId: UUID
)

class RequestExecutor(
    private val client: OkHttpClient,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) {

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

        try {
            client.newCall(request).execute().use { response ->
                val body = try {
                    PaymentExternalServiceImpl.mapper.readValue(
                        response.body?.string(),
                        ExternalSysResponse::class.java
                    )
                } catch (e: Exception) {
                    PaymentExternalServiceImpl.logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                PaymentExternalServiceImpl.logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
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
        }
    }
}