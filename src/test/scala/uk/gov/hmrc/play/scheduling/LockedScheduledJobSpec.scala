/*
 * Copyright 2019 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// /*
//  * Copyright 2019 HM Revenue & Customs
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */

// package uk.gov.hmrc.play.scheduling

// import java.util.concurrent.{CountDownLatch, TimeUnit}
// import java.util.concurrent.atomic.AtomicInteger

// import org.joda.time.{DateTime, DateTimeZone, Duration}
// import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}
// import org.scalatest.concurrent.ScalaFutures
// import org.scalatest.time.{Millis, Seconds, Span}
// import org.scalatestplus.play.guice.GuiceOneAppPerTest
// import play.api.Application
// import play.api.inject.guice.GuiceApplicationBuilder
// import uk.gov.hmrc.lock.{LockMongoRepository, LockRepository}
// import uk.gov.hmrc.play.common.MongoSpecSupportFix

// import scala.concurrent.{Await, ExecutionContext, Future}
// import scala.concurrent.ExecutionContext.Implicits._
// import scala.concurrent.duration._
// import scala.util.Try
// import uk.gov.hmrc.time.DateTimeUtils
// import uk.gov.hmrc.mongo.Awaiting
// class LockedScheduledJobSpec
//     extends WordSpec
//         with Matchers
//         with ScalaFutures
//         with Awaiting
//         //with GuiceOneAppPerTest
//         with MongoSpecSupportFix
//         with BeforeAndAfterEach {
//   private implicit val now = DateTimeUtils.now

//     //  override def fakeApplication(): Application =
//     //    new GuiceApplicationBuilder()
//     //      .configure("mongodb.uri" -> "mongodb://localhost:27017/test-play-schedule")
//     //      .build()

//     //override protected val  mongoUri = s"mongodb://127.0.0.1:27017/$databaseName?rm.monitorRefreshMS=1000&rm.failover=default"
//         //"mongodb://localhost:27017/test-play-schedule"

//     override implicit def patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(1500, Millis), interval = Span(500, Millis))

//     class SimpleJob(val name: String) extends LockedScheduledJob {

//         override val releaseLockAfter = new Duration(300000)

//         val start = new CountDownLatch(1)
//         ///implicit val mongoi = mongo
//         //val lockRepository = new LockRepository()
//         // val lockRepository = new LockRepository()

//       val lockRepository = new LockRepository {
//         val retryIntervalMillis = FiniteDuration(5L, TimeUnit.SECONDS).toMillis

//         override def withCurrentTime[A](f: (DateTime) => A) = f(now)
//       }
//         override protected def beforeEach(): Unit = {
//             await(lockRepository.collection.db.connection.active)
//             await(lockRepository.removeAll())
//         }

//         def continueExecution(): Unit = start.countDown()

//         val executionCount = new AtomicInteger(0)

//         def executions: Int = executionCount.get()

//         override def executeInLock(implicit ec: ExecutionContext): Future[Result] =
//             Future {
//                 start.await(1, TimeUnit.MINUTES)
//                 Result(executionCount.incrementAndGet().toString)
//             }

//         override def initialDelay = FiniteDuration(1, TimeUnit.MINUTES)

//         override def interval = FiniteDuration(1, TimeUnit.MINUTES)

//     }

//     "LockedScheduledJob" should {

//         // "let job run in sequence" in {
//         //   val job = new SimpleJob("job1")
//         //   job.continueExecution()
//         //   Await.result(job.execute, 1.minute).message shouldBe "Job with job1 run and completed with result 1"
//         //   Await.result(job.execute, 1.minute).message shouldBe "Job with job1 run and completed with result 2"
//         // }

//         "not allow job to run in parallel" in {
//             val job = new SimpleJob("job2")

//             val pausedExecution = job.execute
//             pausedExecution.isCompleted shouldBe false
//             Thread.sleep(500)
//             //Await.result(job.isRunning, 1.minute) shouldBe  true
//             //job.isRunning.futureValue       shouldBe true

//             val job2 = new SimpleJob("job2")
//             Await.result(job2.execute, 5.minute).message shouldBe "Job with job2 cannot aquire mongo lock, not running"
//             //job.execute.futureValue.message shouldBe "Job with job2 cannot aquire mongo lock, not running"
//             //job.isRunning.futureValue       shouldBe true

//             job2.continueExecution()
//             Thread.sleep(500)
//             Await.result(pausedExecution, 5.minute).message shouldBe "Job with job2 run and completed with result 1"
//             // pausedExecution.futureValue.message shouldBe "Job with job2 run and completed with result 1"
//             Await.result(job2.isRunning, 5.minute) shouldBe false
//             //job.isRunning.futureValue           shouldBe false
//         }

//         // "should tolerate exceptions in execution" in {
//         //   val job = new SimpleJob("job3") {
//         //     override def executeInLock(implicit ec: ExecutionContext): Future[Result] = throw new RuntimeException
//         //   }

//         //   Try(
//         //  Await.result(job.execute, 1.minute)
//         //     //job.execute.futureValue
//         //   )

//         //  Await.result(job.isRunning, 1.minute) shouldBe false
//         //   // job.isRunning.futureValue shouldBe false
//         // }
//     }

// }
