--*************************************************************************--
-- Title: FinalAssignment
-- Author: <FeiyangYu>
-- Desc: This script backs up three databases
-- Change Log: When,Who,What
-- 2019-08-26,<FeiyangYu>,Created File
--**************************************************************************--



USE [msdb]
GO

/****** Object:  Job [pMaintRefreshThreeDB]    Script Date: 8/24/2019 6:26:24 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 8/24/2019 6:26:24 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'pMaintRefreshThreeDB', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Backups the three dbs and restores a copy of each of them for reporting', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Maint Refresh Three DBs]    Script Date: 8/24/2019 6:26:24 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Maint Refresh Three DBs', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'Execute pMaintRefreshPatients;
Execute pMaintRefreshDoctorsSchedules;
Execute pMaintRefreshDWClinicReportData;


', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'occur daily', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20190824, 
		@active_end_date=99991231, 
		@active_start_time=100, 
		@active_end_time=235959, 
		@schedule_uid=N'3d08b2d7-0901-4e00-83b5-52921db4f6b5'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO



--*************************************************************************--
/*
USE master;
go

If Exists(Select * from Sys.objects where Name = 'pMaintRefreshPatients')
Drop Procedure pMaintRefreshPatients;
go
Create Procedure pMaintRefreshPatients
/* Author: FeiyangYu
** Desc: Backups the Patients db and restores a copy of it for reporting 
** Change Log: When,Who,What
** 2019-08-26,FeiyangYu,Created Sproc.
*/
as
Begin
  Declare @RC int = 0;
  Begin Try
   -- Step 1: Make a copy of the current database
   BACKUP DATABASE [Patients] 
   TO DISK = N'C:\_BISolutions\Patients.bak' 
   WITH INIT;
   -- Step 2: Restore the copy as a different database for reporting
   RESTORE DATABASE [Patients-ReadOnly] 
   FROM DISK = N'C:\_BISolutions\Patients.bak' 
   WITH FILE = 1
      , MOVE N'Patients' TO N'C:\_BISolutions\Patients-Reports.mdf'
      , MOVE N'Patients_log' TO N'C:\_BISolutions\Patients-Reports.ldf'
      , REPLACE;
   -- Step 3: Set the reporting database to read-only
   ALTER DATABASE [Patients-ReadOnly] SET READ_ONLY WITH NO_WAIT;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
End

If Exists(Select * from Sys.objects where Name = 'pMaintRefreshDoctorsSchedules')
Drop Procedure pMaintRefreshDoctorsSchedules;
go
Create Procedure pMaintRefreshDoctorsSchedules
/* Author: FeiyangYu
** Desc: Backups the DoctorsSchedules db and restores a copy of it for reporting 
** Change Log: When,Who,What
** 2019-08-26,FeiyangYu,Created Sproc.
*/
as
Begin
  Declare @RC int = 0;
  Begin Try
   -- Step 1: Make a copy of the current database
   BACKUP DATABASE [DoctorsSchedules] 
   TO DISK = N'C:\_BISolutions\DoctorsSchedules.bak' 
   WITH INIT;
   -- Step 2: Restore the copy as a different database for reporting
   RESTORE DATABASE [DoctorsSchedules-ReadOnly] 
   FROM DISK = N'C:\_BISolutions\DoctorsSchedules.bak' 
   WITH FILE = 1
      , MOVE N'DoctorsSchedules' TO N'C:\_BISolutions\DoctorsSchedules-Reports.mdf'
      , MOVE N'DoctorsSchedules_log' TO N'C:\_BISolutions\DoctorsSchedules-Reports.ldf'
      , REPLACE;
   -- Step 3: Set the reporting database to read-only
   ALTER DATABASE [DoctorsSchedules-ReadOnly] SET READ_ONLY WITH NO_WAIT;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
End

If Exists(Select * from Sys.objects where Name = 'pMaintRefreshDWClinicReportData')
Drop Procedure pMaintRefreshDWClinicReportData;
go
Create Procedure pMaintRefreshDWClinicReportData
/* Author: FeiyangYu
** Desc: Backups the DWClinicReportData db and restores a copy of it for reporting 
** Change Log: When,Who,What
** 2019-08-26,FeiyangYu,Created Sproc.
*/
as
Begin
  Declare @RC int = 0;
  Begin Try
   -- Step 1: Make a copy of the current database
   BACKUP DATABASE [DWClinicReportData] 
   TO DISK = N'C:\_BISolutions\DWClinicReportData.bak' 
   WITH INIT;
   -- Step 2: Restore the copy as a different database for reporting
   RESTORE DATABASE [DWClinicReportData-ReadOnly] 
   FROM DISK = N'C:\_BISolutions\DWClinicReportData.bak' 
   WITH FILE = 1
      , MOVE N'DWClinicReportData' TO N'C:\_BISolutions\DWClinicReportData-Reports.mdf'
      , MOVE N'DWClinicReportData_log' TO N'C:\_BISolutions\DWClinicReportData-Reports.ldf'
      , REPLACE;
   -- Step 3: Set the reporting database to read-only
   ALTER DATABASE [DWClinicReportData-ReadOnly] SET READ_ONLY WITH NO_WAIT;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
End
*/
--**************************************************************************--