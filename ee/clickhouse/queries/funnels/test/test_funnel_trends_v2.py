from datetime import date, datetime, timedelta
from uuid import uuid4

from ee.clickhouse.client import sync_execute
from ee.clickhouse.models.event import create_event
from ee.clickhouse.queries.funnels.funnel_trends import ClickhouseFunnelTrends
from ee.clickhouse.util import ClickhouseTestMixin
from posthog.constants import INSIGHT_FUNNELS, TRENDS_LINEAR
from posthog.models.filters import Filter
from posthog.models.filters.mixins.funnel_window_days import FunnelWindowDaysMixin
from posthog.models.person import Person
from posthog.test.base import APIBaseTest

FORMAT_TIME = "%Y-%m-%d 00:00:00"


def build_funnel_trend_v2_query(team_id: int, start: datetime, event_1: str, event_2: str, event_3: str) -> str:
    JOIN_DISTINCT_ID_SNIPPET = f"""
    JOIN (
        SELECT person_id, distinct_id
        FROM (
            SELECT *
            FROM person_distinct_id
            JOIN (
                SELECT distinct_id, max(_offset) AS _offset
                FROM person_distinct_id
                WHERE team_id = {team_id}
                GROUP BY distinct_id
            ) AS person_max
            ON person_distinct_id.distinct_id = person_max.distinct_id
            AND person_distinct_id._offset = person_max._offset
            WHERE team_id = {team_id}
        )
        WHERE team_id = {team_id}
    ) AS pid
    ON pid.distinct_id = events.distinct_id
    """

    FUNNEL_STEPS_QUERY_3_STEPS = f"""
    SELECT day, countIf(furthest = 1) one_step, countIf(furthest = 2) two_step, countIf(furthest=3) three_step FROM (
        SELECT person_id, toStartOfDay(time_of_event) day, max(steps) AS furthest FROM (
            SELECT *,
            if(rounded_3_date >= rounded_2_date AND rounded_2_date >= latest_1 AND rounded_2_date <= latest_1 + INTERVAL 2 DAY AND rounded_3_date >= latest_1 AND rounded_3_date <= latest_1 + INTERVAL 2 DAY, 3, if(rounded_2_date >= latest_1 AND rounded_2_date <= latest_1 + INTERVAL 2 DAY, 2, 1)) AS steps FROM (
                SELECT
                    person_id,
                    step_1,
                    latest_1,
                    rounded_2,
                    rounded_2_date,
                    rounded_3,
                    min(latest_3) over (PARTITION by person_id ORDER BY time_of_event DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) rounded_3_date,
                    time_of_event
                FROM (
                    SELECT 
                        person_id,
                        step_1,
                        latest_1,
                        rounded_2,
                        rounded_2_date,
                        rounded_3,
                        if(rounded_3_date < rounded_2_date, NULL, rounded_3_date) AS latest_3,
                        time_of_event
                    FROM (
                        SELECT 
                            person_id, 
                            step_1, 
                            latest_1, 
                            step_2 AS rounded_2, 
                            min(latest_2) over (PARTITION by person_id ORDER BY time_of_event DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) rounded_2_date, 
                            step_3 AS rounded_3, 
                            min(latest_3) over (PARTITION by person_id ORDER BY time_of_event DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) rounded_3_date, 
                            time_of_event 
                        FROM (
                            SELECT 
                            pid.person_id AS person_id,
                            if(event = '{event_1}', 1, 0) AS step_1,
                            if(step_1 = 1, timestamp, null) AS latest_1,
                            if(event = '{event_2}', 1, 0) AS step_2,
                            if(step_2 = 1, timestamp, null) AS latest_2,
                            if(event = '{event_3}', 1, 0) AS step_3,
                            if(step_3 = 1, timestamp, null) AS latest_3,
                            if(step_1 = 1 OR step_2 = 1 OR step_3 = 1, timestamp, null) AS time_of_event
                            FROM events
                            {JOIN_DISTINCT_ID_SNIPPET}
                            WHERE team_id = 2
                            AND events.timestamp >= '2021-05-16 00:00:00'
                            AND events.timestamp <= '2021-05-23 23:59:59'
                            AND isNotNull(time_of_event)
                            ORDER BY time_of_event ASC
                        )
                    )
                )
            )
            WHERE step_1 = 1
        ) GROUP BY person_id, day
    ) GROUP BY day
    """

    FUNNEL_TRENDS_QUERY_3_STEPS = f"""
    SELECT toStartOfDay(toDateTime('{start.strftime(FORMAT_TIME)}') - number * 86400) AS day, total, completed, percentage
    FROM numbers(8) AS num
    LEFT OUTER JOIN (
        SELECT day, one_step + three_step AS total, three_step AS completed, completed / total AS percentage FROM (
            {FUNNEL_STEPS_QUERY_3_STEPS}
        )
    ) data
    ON data.day = day 
    ORDER BY day ASC
    SETTINGS allow_experimental_window_functions = 1
    """

    return FUNNEL_TRENDS_QUERY_3_STEPS


def _create_person(**kwargs):
    person = Person.objects.create(**kwargs)
    return Person(id=person.uuid, uuid=person.uuid)


def _create_event(**kwargs):
    kwargs.update({"event_uuid": uuid4()})
    create_event(**kwargs)


class TestFunnelTrends(ClickhouseTestMixin, APIBaseTest):
    def test_all_results_for_day_interval(self):
        time_1 = datetime.fromisoformat("2020-06-13T12:00")
        sql = build_funnel_trend_v2_query(self.team.pk, time_1, "step 1", "step 2", "step 3")
        print(sql)
        results = sync_execute(sql)
        print(results)
