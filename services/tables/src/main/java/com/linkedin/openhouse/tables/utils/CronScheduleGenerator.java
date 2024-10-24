package com.linkedin.openhouse.tables.utils;

import com.cronutils.builder.CronBuilder;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.field.expression.FieldExpressionFactory;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Utility class for generating cron schedules based on interval input */
@Slf4j
@Component
public class CronScheduleGenerator {

  /**
   * Public api to generate a cron schedule for a {@link ReplicationConfig} based on the interval
   *
   * @param interval
   * @return schedule
   */
  public static String buildCronExpression(String interval) {
    int count = Integer.parseInt(interval.substring(0, interval.length() - 1));
    String granularity = interval.substring(interval.length() - 1);
    int hour = new Random().nextInt(24);
    String schedule;

    if (granularity.equals("H")) {
      schedule = buildHourlyCronExpression(hour, count);
    } else {
      schedule = buildDailyCronExpression(hour, count);
    }
    return schedule;
  }

  private static String buildDailyCronExpression(int hour, int dailyInterval) {
    return CronBuilder.cron(CronDefinitionBuilder.instanceDefinitionFor(CronType.CRON4J))
        .withDoM(FieldExpressionFactory.every(dailyInterval))
        .withMonth(FieldExpressionFactory.always())
        .withDoW(FieldExpressionFactory.always())
        .withHour(FieldExpressionFactory.on(hour))
        .withMinute(FieldExpressionFactory.on(0))
        .instance()
        .asString();
  }

  private static String buildHourlyCronExpression(int hour, int hourlyInterval) {
    return CronBuilder.cron(CronDefinitionBuilder.instanceDefinitionFor(CronType.CRON4J))
        .withDoM(FieldExpressionFactory.always())
        .withMonth(FieldExpressionFactory.always())
        .withDoW(FieldExpressionFactory.always())
        .withHour(FieldExpressionFactory.every(hourlyInterval).and(FieldExpressionFactory.on(hour)))
        .withMinute(FieldExpressionFactory.on(0))
        .instance()
        .asString();
  }
}
