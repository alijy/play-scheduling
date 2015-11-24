/*
 * Copyright 2015 HM Revenue & Customs
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

package uk.gov.hmrc.play.scheduling

import java.util.concurrent.TimeUnit

import akka.actor.{Cancellable, Scheduler}
import org.apache.commons.lang3.time.StopWatch
import org.joda.time.{DateTime, Seconds}
import play.api.libs.concurrent.Akka
import play.api.{Application, GlobalSettings, Logger}

import scala.concurrent.duration._
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.util.{Failure, Success}

trait RunningOfScheduledJobs extends GlobalSettings {
  def scheduler(app: Application): Scheduler = Akka.system(app).scheduler

  val scheduledJobs: Seq[ScheduledJob]

  private[scheduling] var cancellables: Seq[Cancellable] = Seq.empty

  override def onStart(app: Application) {
    super.onStart(app)

    implicit val ec = play.api.libs.concurrent.Execution.defaultContext

    def toDateTime(schedule: String)(implicit now: DateTime = DateTime.now): DateTime = {
      val timeArr = schedule.trim.split(":")
      val (hour, minute) = (timeArr(0).toInt, timeArr(1).toInt)
      now.withHourOfDay(hour)
        .withMinuteOfHour(minute)
        .withSecondOfMinute(0)
        .withMillisOfSecond(0)
    }

    class ScheduledJobWithDelay(job: ScheduledJob, delay: Int) extends ScheduledJob {
      def name = job.name

      def execute(implicit ec: ExecutionContext) = Future.successful(Result("done"))

      def interval = job.interval

      def initialDelay = FiniteDuration(delay, TimeUnit.SECONDS)

      def isRunning = job.isRunning
    }

    def executeScheduler(job: ScheduledJob): Cancellable = {
      scheduler(app).schedule(job.initialDelay, job.interval) {

        val stopWatch = new StopWatch
        stopWatch.start()

        Logger.info(s"Scheduling jobs: $job")

        Logger.info(s"Executing job ${job.name}")

        job.execute.onComplete {
          case Success(job.Result(message)) =>
            stopWatch.stop()
            Logger.info(s"Completed job ${job.name} in $stopWatch: $message")
          case Failure(throwable) =>
            stopWatch.stop()
            Logger.error(s"Exception running job ${job.name} after $stopWatch", throwable)
        }
      }
    }

    cancellables = scheduledJobs.map { job =>

      (job.specifiedSchedules) match {
        case Some(specifiedSchedule) =>
          for {
            convertedSchedule <- specifiedSchedule.split(",").map(toDateTime);
            delay = Seconds.secondsBetween(DateTime.now, convertedSchedule).getSeconds;
            jobWithDelay = new ScheduledJobWithDelay(job, delay)
          } yield executeScheduler(jobWithDelay)
        case None => Array(executeScheduler(job))
      }
    }.flatten

  }

  override def onStop(app: Application) {
    Logger.info(s"Cancelling all scheduled jobs.")
    cancellables.foreach(_.cancel())
    scheduledJobs.foreach { job =>
      Logger.info(s"Checking if job ${job.configKey} is running")
      while (Await.result(job.isRunning, 5.seconds)) {
        Logger.warn(s"Waiting for job ${job.configKey} to finish")
        Thread.sleep(1000)
      }
      Logger.warn(s"Job ${job.configKey} is finished")
    }
  }
}
