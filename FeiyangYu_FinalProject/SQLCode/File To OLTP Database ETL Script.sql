--*************************************************************************--
-- Title: FinalAssignment
-- Author: <FeiyangYu>
-- Desc: This script imports file to OLTP database
-- Change Log: When,Who,What
-- 2019-08-26,<FeiyangYu>,Created File
--**************************************************************************--

USE [Patients];
go

If (Select Object_ID('pETLCreateOrClearStagingTables')) is NOT null
Drop Procedure pETLCreateOrClearStagingTables;
go

Create Procedure pETLCreateOrClearStagingTables
/* Author: <FeiyangYu>
** Desc: Create or clear all the staging tables
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	If (Select OBJECT_ID('BellevueStagingTable')) Is NOT null
		Truncate Table BellevueStagingTable;
	Else
		Create Table [dbo].[BellevueStagingTable](
		[Time] [varchar](100) NULL,
		--[Clinic] [varchar](100) NULL,
		[Patient] [varchar](100) NULL,
		[Doctor] [varchar](100) NULL,
		[Procedure] [varchar](100) NULL,
		[Charge] [varchar](100) NULL
		);

	If (Select OBJECT_ID('KirklandStagingTable')) Is NOT null
		Truncate Table KirklandStagingTable;
	Else
		Create Table [dbo].[KirklandStagingTable](
		[Time] [varchar](100) NULL,
		[Clinic] [varchar](100) NULL,
		[Patient] [varchar](100) NULL,
		[Doctor] [varchar](100) NULL,
		[Procedure] [varchar](100) NULL,
		[Charge] [varchar](100) NULL
		);

	If (Select OBJECT_ID('RedmondStagingTable')) Is NOT null
		Truncate Table RedmondStagingTable;
	Else
		Create Table [dbo].[RedmondStagingTable](
		[Time] [varchar](100) NULL,
		[Clinic] [varchar](100) NULL,
		[Patient] [varchar](100) NULL,
		[Doctor] [varchar](100) NULL,
		[Procedure] [varchar](100) NULL,
		[Charge] [varchar](100) NULL
		);
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

--Execute pETLCreateOrClearStagingTables;
--go


If (Select Object_ID('pETLImportStagingTables')) is NOT null
Drop Procedure pETLImportStagingTables;
go

Create Procedure pETLImportStagingTables
/* Author: <FeiyangYu>
** Desc: import data to the staging tables
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	BULK INSERT Patients.dbo.[BellevueStagingTable]
	 FROM 'C:\DataToProcess\Bellevue\20100102Visits.csv'
	  WITH ( FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW=2);
	  	
	BULK INSERT Patients.dbo.[KirklandStagingTable]
	 FROM 'C:\DataToProcess\Kirkland\20100102Visits.csv'
	  WITH ( FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW=2);

	BULK INSERT Patients.dbo.[RedmondStagingTable]
	 FROM 'C:\DataToProcess\Redmond\20100102Visits.csv'
	  WITH ( FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW=2);
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

--Execute pETLImportStagingTables;
--go



If (Select Object_ID('vETLNewVisitData')) is NOT null
Drop View vETLNewVisitData;
go

Create View vETLNewVisitData
/* Author: <FeiyangYu>
** Desc: Create view for new visits data
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
As
  Select 
    [Time], [Clinic] = 1, [Patient], [Doctor], 
	[Procedure], [Charge] 
   from dbo.[BellevueStagingTable]
  Union All
  Select 
    [Time], [Patient], [Clinic], [Doctor], 
	[Procedure], [Charge] 
   from dbo.[KirklandStagingTable]
  Union All
  Select 
    [Time], [Clinic], [Patient], [Doctor], 
	[Procedure], [Charge] 
   from dbo.[RedmondStagingTable]
go

--Select * From vETLNewVisitData
--go


If (Select Object_ID('pETLImportNewVisitData')) is NOT null
Drop procedure pETLImportNewVisitData;
go

Create Procedure pETLImportNewVisitData(@Date date)
/* Author: <FeiyangYu>
** Desc: import data to the patients database
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
		With AddedORChangedVisitData
		As(
			Select 
			[Date] = CAST(@Date as datetime) + CAST([Time] as datetime),
			[Clinic] = CAST([Clinic] as int) * 100,
			[Patient] = CAST([Patient] as int),
			[Doctor] = CAST([Doctor] as int),
			[Procedure] = CAST([Procedure] as int),
			[Charge] = CAST([Charge] as money)
			From vETLNewVisitData
			Except
			Select 
			[Date], [Clinic], [Patient], [Doctor], [Procedure], [Charge] 
			From Patients.dbo.Visits

		)
	  INSERT INTO [Patients].dbo.Visits
      ([Date],[Clinic],[Patient],[Doctor],[Procedure],[Charge])
      SELECT
        [Date]
       ,[Clinic]
       ,[Patient]
       ,[Doctor]
       ,[Procedure] 
       ,[Charge]
      FROM AddedORChangedVisitData;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

--select * from Visits
--Declare @Date date = Convert(datetime, convert(char(8), '20100102'))
--EXEC pETLImportNewVisitData @Date = @Date
--go
--delete from Visits where cast(format(Date, 'yyyyMMdd') as int) = '20100102'
--delete from Visits where YEAR(Date) = 2019



Declare @Status int = 0;
Exec @Status = pETLCreateOrClearStagingTables;
Select [Object] = 'pETLCreateOrClearStagingTables', [Status] = @Status;

Exec @Status = pETLImportStagingTables;
Select [Object] = 'pETLImportStagingTables', [Status] = @Status;

Declare @Date date = Convert(datetime, convert(char(8), '20100102'))
Exec @Status = pETLImportNewVisitData @Date = @Date;
Select [Object] = 'pETLImportNewVisitData', [Status] = @Status;
