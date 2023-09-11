from typing import List


class AsyncResponse:
    def __init__(self, task_name: str, task_success: bool):
        self.task_name = task_name
        self.task_success = task_success


class AsyncResponseOcorrencia(AsyncResponse):
    def __init__(self, task_name: str, task_success: bool, ocorrencia_id: int, data: dict):
        super().__init__(task_name, task_success)
        self.ocorrencia_id = ocorrencia_id
        self.data = data


class AsyncResponseS3(AsyncResponse):
    def __init__(self, task_name: str, task_success: bool, s3_path: str):
        super().__init__(task_name, task_success)
        self.s3_path = s3_path


class AsyncResponseMonitoramento(AsyncResponse):
    def __init__(self, task_name: str, task_success: bool, monitoramento_id: int, data: dict):
        super().__init__(task_name, task_success)
        self.monitoramento_id = monitoramento_id
        self.data = data


def get_failed_async_tasks_name(response_list: List[AsyncResponse]) -> List:
    return [response.name for response in response_list if not response.task_success]
