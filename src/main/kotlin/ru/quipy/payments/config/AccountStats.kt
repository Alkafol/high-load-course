package ru.quipy.payments.config

import okhttp3.Request
import org.springframework.stereotype.Service
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.RequestData
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min

class AccountProperties (
    val speed: Long?,
    val maxSize: Long = speed?.times(80) ?: error("Invalid speed"),
    val curRequestsAmount: AtomicInteger = AtomicInteger(0),
    val queue: ConcurrentLinkedQueue<RequestData> = ConcurrentLinkedQueue(),
    val pool: ExecutorService = Executors.newFixedThreadPool(10),
    val extProperties: ExternalServiceProperties
)

@Service
class AccountStatisticsService {
    val statistics = ExternalServicesConfig.getAll().associateBy(
        { it }, {
            AccountProperties(
                speed = min(
                    it.rateLimitPerSec.toLong(),
                    it.parallelRequests * 1000 / it.request95thPercentileProcessingTime.toMillis()
                ),
                extProperties = it
            )
        }
    )

    fun getProperties(account: ExternalServiceProperties): AccountProperties {
        return statistics[account] ?: error("Invalid account: ${account.accountName}")
    }

    fun trySendRequest(account: ExternalServiceProperties): Boolean {
        val curAccountStats = statistics[account] ?: error("No statistics for account: ${account.accountName}")
        val curAccountRequests = curAccountStats.curRequestsAmount.get()

        if (curAccountRequests >= account.parallelRequests * 80 * account.rateLimitPerSec) {
            return false
        }

        return curAccountStats.curRequestsAmount.compareAndSet(curAccountRequests, curAccountRequests + 1)
    }

    fun receiveResponse(account: ExternalServiceProperties) {
        statistics[account]?.curRequestsAmount?.decrementAndGet()
            ?: error("No statistics for account: ${account.accountName}")
    }
}