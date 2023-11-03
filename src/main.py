from datetime import datetime
from enum import Enum
from motor.motor_asyncio import AsyncIOMotorCollection
from pydantic import BaseModel, ConfigDict
import pandas


class UnitTime(str, Enum):
    hour: str = "hour"
    month: str = "month"
    day: str = "day"


class AggregationPipeline(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    dt_from: datetime
    dt_upto: datetime
    group_type: UnitTime
    pipeline: list = []
    collection: AsyncIOMotorCollection
    DATE_FORMATS: dict = {
        "month": "%Y-%m-01T00:00:00",
        "day": "%Y-%m-%dT00:00:00",
        "hour": "%Y-%m-%dT%H:00:00",
    }

    def _add_match(self):
        self.pipeline.append(
            {
                "$match": {
                    "dt":
                        {"$gte": self.dt_from, "$lte": self.dt_upto}
                }
            }
        )

    def _add_group(self):
        self.pipeline.append(
            {
                "$group": {
                    "_id": {"$dateToString": {"format": self.DATE_FORMATS[self.group_type],
                                              "date": "$dt"}
                            },
                    "total": {"$sum": "$value"}
                }
            }
        )

    def _add_sort(self):
        self.pipeline.append(
            {
                "$sort": {"_id": 1}
            }
        )

    def _aggregation_result(self):
        self._add_match()
        self._add_group()
        self._add_sort()
        return self.collection.aggregate(pipeline=self.pipeline)

    async def result_aggregation(self):
        data_aggregation = []
        res = (pandas.date_range(start=self.dt_from,
                                 end=self.dt_upto,
                                 freq=self.group_type[0])
               .strftime(self.DATE_FORMATS[self.group_type]).tolist()) # Список дат из интервала
        date_aggregate_generator = self._generator_get_aggregation()
        date_aggregate = await anext(
            date_aggregate_generator)  # Получаем первую дату {"_id": datetime "total": int} из генератора результата агрегации
        for date in res:
            date = datetime.fromisoformat(date)
            if date < date_aggregate['_id']:  # Если дата из интервала в бд отсутствует, то добавляем
                data_aggregation.append([date.isoformat(), 0])
            elif date > date_aggregate['_id']:  # Если дата из интервала в бд отсутствует, то добавляем
                data_aggregation.append([date.isoformat(), 0])
            else:
                data_aggregation.append([date_aggregate['_id'].isoformat(), date_aggregate['total']])
                try:
                    date_aggregate = await anext(
                        date_aggregate_generator)  # Получает след запись {"_id": datetime "total": int} из генератора
                except StopAsyncIteration:
                    continue

        labels, dataset = zip(*data_aggregation)
        return {"dataset": list(dataset), "labels": list(labels)}

    async def _generator_get_aggregation(self):
        aggregation_result = self._aggregation_result()
        async for i in aggregation_result:
            yield {"_id": datetime.fromisoformat(i["_id"]),
                   "total": i['total']}
