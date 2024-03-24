package ru.quipy.payments.config

import org.springframework.stereotype.Service
import ru.quipy.payments.logic.ExternalServiceProperties
import java.util.concurrent.atomic.AtomicInteger

class AccountStatistics {
    val curRequestsAmount = AtomicInteger(0)
}

@Service
class AccountStatisticsService {
    val statistics = ExternalServicesConfig.getAll().associateBy(
        { it }, { AccountStatistics() }
    )

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