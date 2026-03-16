# LogCopilot Top Clusters

- Events: 23223
- Signature clusters: 1195

## Top-10 incidents

### 1. fa1582cf0c1899a9a1d4c38fddf955d450c62f01
- Hits: 468
- Incident hits: 468
- First seen: 2026-03-11 09:13:00.043000
- Last seen: 2026-03-11 15:42:00.013000
- Levels: ERROR:468
- Exception: Npgsql.PostgresException
- Source files: BusinessProcess.log (468)
- Sample messages:
  - Npgsql.PostgresException (0x80004005): 23503: insert or update on table "SysProcessElementLog" violates foreign key constraint "FKTQLhPckOuXfL4S8XTK2ODa6vErc"

### 2. bcf7ccfa3769e3c11c7d4e5d73836d32162d8e6d
- Hits: 441
- Incident hits: 441
- First seen: 2026-03-11 08:22:34.724000
- Last seen: 2026-03-11 15:42:00.009000
- Levels: ERROR:441
- Exception: Npgsql.PostgresException
- Source files: BusinessProcess.log (441)
- Sample messages:
  - Error in process "Name = BPMSoftOCCSaveAfkChats UId = 873de002-c645-4ee2-b9ce-a8c949719e23": Npgsql.PostgresException (0x80004005): 23503: insert or update on table "SysProcessElementLog" violates foreign key constraint "FKTQLhPckOuXfL4S8XTK2ODa6vErc"
  - Error in process "Name = BPMSoftOCCSaveAfkChats UId = ebc799f8-f621-4647-aa4b-fc5d07e4d01b": Npgsql.PostgresException (0x80004005): 23503: insert or update on table "SysProcessElementLog" violates foreign key constraint "FKTQLhPckOuXfL4S8XTK2ODa6vErc"
  - Error in process "Name = BPMSoftOCCSaveAfkChats UId = 1facfb79-b8d5-4237-b510-348b035e6995": Npgsql.PostgresException (0x80004005): 23503: insert or update on table "SysProcessElementLog" violates foreign key constraint "FKTQLhPckOuXfL4S8XTK2ODa6vErc"
  - Error in process "Name = BPMSoftOCCSaveAfkChats UId = e14ef7b3-9475-4afe-b27c-13523468f3df": Npgsql.PostgresException (0x80004005): 23503: insert or update on table "SysProcessElementLog" violates foreign key constraint "FKTQLhPckOuXfL4S8XTK2ODa6vErc"
  - Error in process "Name = BPMSoftOCCSaveAfkChats UId = a367fa47-8a85-4c77-a5f6-0c1b5c5c5d09": Npgsql.PostgresException (0x80004005): 23503: insert or update on table "SysProcessElementLog" violates foreign key constraint "FKTQLhPckOuXfL4S8XTK2ODa6vErc"

### 3. a41cf6f75ef368f7a4bf4b800bd6eb4d5c58d4ea
- Hits: 89
- Incident hits: 89
- First seen: 2026-03-11 08:22:34.724000
- Last seen: 2026-03-11 15:40:00.008000
- Levels: ERROR:89
- Exception: Npgsql.PostgresException
- Source files: BusinessProcess.log (89)
- Sample messages:
  - Error in process "Name = BPMSoftOCCCloseOldChats UId = a09c4eea-f280-49c3-bb12-e8f9d8d94d70": Npgsql.PostgresException (0x80004005): 23503: insert or update on table "SysProcessElementLog" violates foreign key constraint "FKTQLhPckOuXfL4S8XTK2ODa6vErc"
  - Error in process "Name = BPMSoftOCCCloseOldChats UId = 7893b6b1-4856-4506-adcb-3c2c423702ea": Npgsql.PostgresException (0x80004005): 23503: insert or update on table "SysProcessElementLog" violates foreign key constraint "FKTQLhPckOuXfL4S8XTK2ODa6vErc"
  - Error in process "Name = BPMSoftOCCCloseOldChats UId = a406c453-dce4-4d75-a4dd-a9732a28f6fc": Npgsql.PostgresException (0x80004005): 23503: insert or update on table "SysProcessElementLog" violates foreign key constraint "FKTQLhPckOuXfL4S8XTK2ODa6vErc"
  - Error in process "Name = BPMSoftOCCCloseOldChats UId = 6842ae04-7334-47af-916d-82c6b477a80f": Npgsql.PostgresException (0x80004005): 23503: insert or update on table "SysProcessElementLog" violates foreign key constraint "FKTQLhPckOuXfL4S8XTK2ODa6vErc"
  - Error in process "Name = BPMSoftOCCCloseOldChats UId = 4823f0e4-276b-4026-9208-6fe8d9367318": Npgsql.PostgresException (0x80004005): 23503: insert or update on table "SysProcessElementLog" violates foreign key constraint "FKTQLhPckOuXfL4S8XTK2ODa6vErc"

### 4. aeca61d6a830206393bf9671a31580cb9527e275
- Hits: 86
- Incident hits: 86
- First seen: 2026-03-11 08:35:03.935000
- Last seen: 2026-03-11 15:40:03.942000
- Levels: ERROR:86
- Exception: System.ArgumentNullException
- Source files: Scheduler.log (86)
- Sample messages:
  - JobFail [className:LlmGigaChatJob, Exception:BPMSoft.Core.InstanceActivationException: Ошибка создания экземпляра класса "BPMSoft.Core.IJobExecutor"

### 5. 92cd1be040c84d155bb79ac10625eb0f2282accb
- Hits: 86
- Incident hits: 86
- First seen: 2026-03-11 08:35:03.935000
- Last seen: 2026-03-11 15:40:03.942000
- Levels: ERROR:86
- Exception: BPMSoft.Core.InstanceActivationException
- Source files: Scheduler.log (86)
- Sample messages:
  - Error executing [BPMSoft.Core.Scheduler.RunAppJob] in context [JobExecutionContext: trigger: 'DEFAULT.fcea1eff-9895-4a7f-9dea-36a3fc0887dd' job: 'LlmGigaChatJob.BPMSoft.Configuration.AI.LlmGigaChatJob' fireTimeUtc: 'Wed, 11 Mar 2026 08:35:03 GMT' scheduledFireTimeUtc: 'Wed, 11 Mar 2026 08:35:03 GMT' previousFireTimeUtc: 'Wed, 11 Mar 2026 08:30:03 GMT' nextFireTimeUtc: 'Wed, 11 Mar 2026 08:40:03 GMT' recovering: False refireCount: 0]
  - Error executing [BPMSoft.Core.Scheduler.RunAppJob] in context [JobExecutionContext: trigger: 'DEFAULT.fcea1eff-9895-4a7f-9dea-36a3fc0887dd' job: 'LlmGigaChatJob.BPMSoft.Configuration.AI.LlmGigaChatJob' fireTimeUtc: 'Wed, 11 Mar 2026 08:40:03 GMT' scheduledFireTimeUtc: 'Wed, 11 Mar 2026 08:40:03 GMT' previousFireTimeUtc: 'Wed, 11 Mar 2026 08:35:03 GMT' nextFireTimeUtc: 'Wed, 11 Mar 2026 08:45:03 GMT' recovering: False refireCount: 0]
  - Error executing [BPMSoft.Core.Scheduler.RunAppJob] in context [JobExecutionContext: trigger: 'DEFAULT.fcea1eff-9895-4a7f-9dea-36a3fc0887dd' job: 'LlmGigaChatJob.BPMSoft.Configuration.AI.LlmGigaChatJob' fireTimeUtc: 'Wed, 11 Mar 2026 08:45:03 GMT' scheduledFireTimeUtc: 'Wed, 11 Mar 2026 08:45:03 GMT' previousFireTimeUtc: 'Wed, 11 Mar 2026 08:40:03 GMT' nextFireTimeUtc: 'Wed, 11 Mar 2026 08:50:03 GMT' recovering: False refireCount: 0]
  - Error executing [BPMSoft.Core.Scheduler.RunAppJob] in context [JobExecutionContext: trigger: 'DEFAULT.fcea1eff-9895-4a7f-9dea-36a3fc0887dd' job: 'LlmGigaChatJob.BPMSoft.Configuration.AI.LlmGigaChatJob' fireTimeUtc: 'Wed, 11 Mar 2026 08:50:03 GMT' scheduledFireTimeUtc: 'Wed, 11 Mar 2026 08:50:03 GMT' previousFireTimeUtc: 'Wed, 11 Mar 2026 08:45:03 GMT' nextFireTimeUtc: 'Wed, 11 Mar 2026 08:55:03 GMT' recovering: False refireCount: 0]
  - Error executing [BPMSoft.Core.Scheduler.RunAppJob] in context [JobExecutionContext: trigger: 'DEFAULT.fcea1eff-9895-4a7f-9dea-36a3fc0887dd' job: 'LlmGigaChatJob.BPMSoft.Configuration.AI.LlmGigaChatJob' fireTimeUtc: 'Wed, 11 Mar 2026 08:55:03 GMT' scheduledFireTimeUtc: 'Wed, 11 Mar 2026 08:55:03 GMT' previousFireTimeUtc: 'Wed, 11 Mar 2026 08:50:03 GMT' nextFireTimeUtc: 'Wed, 11 Mar 2026 09:00:03 GMT' recovering: False refireCount: 0]

### 6. 586e5fa236eb592c2f837dbb469c327414578dfe
- Hits: 62
- Incident hits: 62
- First seen: 2026-03-11 08:22:34.735000
- Last seen: 2026-03-11 09:12:00.022000
- Levels: ERROR:62
- Exception: BPMSoft.Core.Process.ProcessComponentSet.WriteElementError
- Source files: BusinessProcess.log (62)
- Sample messages:
  - Npgsql.PostgresException (0x80004005): 23503: insert or update on table "SysProcessElementLog" violates foreign key constraint "FKTQLhPckOuXfL4S8XTK2ODa6vErc"

### 7. b9fdcd8e5d7d7d8c016083c5486cb18dc1921463
- Hits: 31
- Incident hits: 31
- First seen: 2026-03-11 08:22:34.817000
- Last seen: 2026-03-11 15:37:36.316000
- Levels: ERROR:31
- Exception: System.ServiceModel.ServiceActivationException
- Source files: Scheduler.log (31)
- Sample messages:
  - JobFail [className:BPMSoft.Configuration.ML.MLModelTrainerJob, BPMSoft.Configuration, Version=1.8.0.14103, Culture=neutral, PublicKeyToken=null, Exception:System.ServiceModel.ServiceActivationException: Set AspNetCompatibilityEnabled true

### 8. ea16d89c34439608d2f52addeeb3f450fe5ef40d
- Hits: 31
- Incident hits: 31
- First seen: 2026-03-11 08:22:34.808000
- Last seen: 2026-03-11 15:37:36.311000
- Levels: ERROR:31
- Exception: System.ServiceModel.ServiceActivationException
- Source files: ML/ML.log (31)
- Sample messages:
  - Exception was thrown during ML model trainer job

### 9. defb5262b1957ec44e3ecd4036f5b21ef87734fe
- Hits: 30
- Incident hits: 30
- First seen: 2026-03-11 08:22:34.817000
- Last seen: 2026-03-11 15:37:36.316000
- Levels: ERROR:30
- Exception: System.ServiceModel.ServiceActivationException
- Source files: Scheduler.log (30)
- Sample messages:
  - Error executing [BPMSoft.Core.Scheduler.RunAppJob] in context [JobExecutionContext: trigger: 'DEFAULT.BPMSoft.Configuration.ML.MLModelTrainerJob, BPMSoft.Configuration, Version=1.8.0.14103, Culture=neutral, PublicKeyToken=nullTrigger' job: 'MLModelTrainerJob.BPMSoft.Configuration.ML.MLModelTrainerJob, BPMSoft.Configuration, Version=1.8.0.14103, Culture=neutral, PublicKeyToken=null' fireTimeUtc: 'Wed, 11 Mar 2026 08:22:34 GMT' scheduledFireTimeUtc: 'Wed, 11 Mar 2026 08:22:32 GMT' previousFireTimeUtc: '' nextFireTimeUtc: '' recovering: False refireCount: 0]
  - Error executing [BPMSoft.Core.Scheduler.RunAppJob] in context [JobExecutionContext: trigger: 'DEFAULT.BPMSoft.Configuration.ML.MLModelTrainerJob, BPMSoft.Configuration, Version=1.8.0.14103, Culture=neutral, PublicKeyToken=nullTrigger' job: 'MLModelTrainerJob.BPMSoft.Configuration.ML.MLModelTrainerJob, BPMSoft.Configuration, Version=1.8.0.14103, Culture=neutral, PublicKeyToken=null' fireTimeUtc: 'Wed, 11 Mar 2026 08:37:35 GMT' scheduledFireTimeUtc: 'Wed, 11 Mar 2026 08:37:35 GMT' previousFireTimeUtc: '' nextFireTimeUtc: '' recovering: False refireCount: 0]
  - Error executing [BPMSoft.Core.Scheduler.RunAppJob] in context [JobExecutionContext: trigger: 'DEFAULT.BPMSoft.Configuration.ML.MLModelTrainerJob, BPMSoft.Configuration, Version=1.8.0.14103, Culture=neutral, PublicKeyToken=nullTrigger' job: 'MLModelTrainerJob.BPMSoft.Configuration.ML.MLModelTrainerJob, BPMSoft.Configuration, Version=1.8.0.14103, Culture=neutral, PublicKeyToken=null' fireTimeUtc: 'Wed, 11 Mar 2026 08:52:35 GMT' scheduledFireTimeUtc: 'Wed, 11 Mar 2026 08:52:35 GMT' previousFireTimeUtc: '' nextFireTimeUtc: '' recovering: False refireCount: 0]
  - Error executing [BPMSoft.Core.Scheduler.RunAppJob] in context [JobExecutionContext: trigger: 'DEFAULT.BPMSoft.Configuration.ML.MLModelTrainerJob, BPMSoft.Configuration, Version=1.8.0.14103, Culture=neutral, PublicKeyToken=nullTrigger' job: 'MLModelTrainerJob.BPMSoft.Configuration.ML.MLModelTrainerJob, BPMSoft.Configuration, Version=1.8.0.14103, Culture=neutral, PublicKeyToken=null' fireTimeUtc: 'Wed, 11 Mar 2026 09:07:35 GMT' scheduledFireTimeUtc: 'Wed, 11 Mar 2026 09:07:35 GMT' previousFireTimeUtc: '' nextFireTimeUtc: '' recovering: False refireCount: 0]
  - Error executing [BPMSoft.Core.Scheduler.RunAppJob] in context [JobExecutionContext: trigger: 'DEFAULT.BPMSoft.Configuration.ML.MLModelTrainerJob, BPMSoft.Configuration, Version=1.8.0.14103, Culture=neutral, PublicKeyToken=nullTrigger' job: 'MLModelTrainerJob.BPMSoft.Configuration.ML.MLModelTrainerJob, BPMSoft.Configuration, Version=1.8.0.14103, Culture=neutral, PublicKeyToken=null' fireTimeUtc: 'Wed, 11 Mar 2026 09:22:35 GMT' scheduledFireTimeUtc: 'Wed, 11 Mar 2026 09:22:35 GMT' previousFireTimeUtc: '' nextFireTimeUtc: '' recovering: False refireCount: 0]

### 10. f7381d044fe9fc1bedf95f287dfb82e776e98e42
- Hits: 9
- Incident hits: 9
- First seen: 2026-03-11 08:22:34.828000
- Last seen: 2026-03-11 15:22:36.009000
- Levels: ERROR:9
- Exception: System.ServiceModel.ServiceActivationException
- Source files: Scheduler.log (9)
- Sample messages:
  - JobFail [className:BPMSoft.Configuration.ML.MLBatchPredictionJob, BPMSoft.Configuration, Version=1.8.0.14103, Culture=neutral, PublicKeyToken=null, Exception:System.ServiceModel.ServiceActivationException: Set AspNetCompatibilityEnabled true
