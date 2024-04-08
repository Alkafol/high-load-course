package ru.quipy.payments.config

import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Protocol
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CustomPolicy
import ru.quipy.common.utils.RateLimiter
import ru.quipy.payments.logic.ExternalServiceProperties
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min

class AccountProperties (
    val speed: Double?,
    val maxSize: Long = speed?.times(80)?.toLong() ?: error("Invalid speed"),
    val curRequestsAmount: AtomicInteger = AtomicInteger(0),
    val queue: BlockingQueue<Runnable> = ArrayBlockingQueue(1000),
    val extProperties: ExternalServiceProperties,

    val limiter:RateLimiter = RateLimiter(extProperties.rateLimitPerSec),
    val pool: ThreadPoolExecutor = ThreadPoolExecutor(
        3,
        20,
        10,
        TimeUnit.SECONDS,
        queue,
        CustomizableThreadFactory("${extProperties.accountName}_"),
        CustomPolicy()
    )
)

@Service
class AccountStatisticsService {
    val statistics = ExternalServicesConfig.getAll().associateBy(
        { it }, {
            AccountProperties(
                speed = min(
                    it.rateLimitPerSec.toDouble(),
                    it.parallelRequests * 1.0 / it.request95thPercentileProcessingTime.seconds
                ),
                extProperties = it
            )
        }
    )

    val client = OkHttpClient.Builder()
        .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
        .run {
            dispatcher(
                Dispatcher(Executors.newFixedThreadPool(100)
            ).apply {
                    maxRequestsPerHost = 50
                    maxRequests = 50
                }
            )
            build()
        }

    fun getProperties(account: ExternalServiceProperties): AccountProperties {
        return statistics[account] ?: error("Invalid account: ${account.accountName}")
    }

    fun trySendRequest(account: ExternalServiceProperties): Boolean {
        while (true) {
            val curAccountStats = statistics[account] ?: error("No statistics for account: ${account.accountName}")
            val curAccountRequests = curAccountStats.curRequestsAmount.get()

            if (curAccountRequests >= curAccountStats.maxSize) {
                return false
            }

            if (curAccountStats.curRequestsAmount.compareAndSet(curAccountRequests, curAccountRequests + 1)) {
                return true
            }
        }
    }

    fun receiveResponse(account: ExternalServiceProperties) {
        statistics[account]?.curRequestsAmount?.decrementAndGet()
            ?: error("No statistics for account: ${account.accountName}")
    }
}