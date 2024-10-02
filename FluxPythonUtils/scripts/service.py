from pendulum import DateTime


class Service(object):
    """
    should ideally be first in multi inheritance or must have all preceding classes designed for multi-inheritance
    """
    def __init__(self, **kwargs):  #
        # super: refers next in MRO; super called with kwargs allows both arg/no-arg inheritors subsequently
        super().__init__(**kwargs)
        # prevents consuming any market data older than current time
        self.service_start_time: DateTime = DateTime.now()
        # stabilization period: mark repeat logic related errors as errors after this, otherwise they can be Info/Warn
        self.stabilization_period_in_min: int = 2
        self.stabilization_period_past_service_start: bool = False

    def is_stabilization_period_past(self) -> bool:
        if not self.stabilization_period_past_service_start:
            cur_time: DateTime = DateTime.now()
            period = self.service_start_time - cur_time
            delta_in_min = (period.as_timedelta().total_seconds()) / 60
            if delta_in_min > self.stabilization_period_in_min:
                self.stabilization_period_past_service_start = True
                return self.stabilization_period_past_service_start
        # else not needed just return stabilization_period_past_executor_start is sufficient - it's already false
        return self.stabilization_period_past_service_start
