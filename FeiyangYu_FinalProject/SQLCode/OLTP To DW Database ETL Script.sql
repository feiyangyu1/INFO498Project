--*************************************************************************--
-- Title: FinalAssignment
-- Author: <FeiyangYu>
-- Desc: This script imports OLTP data to data warehouse
-- Change Log: When,Who,What
-- 2019-08-26,<FeiyangYu>,Created File
--**************************************************************************--

USE [DWClinicReportData];
go

If Exists(Select * from Sys.objects where Name = 'pETLDropForeignKeyConstraints')
   Drop Procedure pETLDropForeignKeyConstraints;
go
	If Exists(Select * from Sys.objects where Name = 'pETLTruncateTables')
   Drop Procedure pETLTruncateTables;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimDates')
   Drop Procedure pETLFillDimDates;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimDoctors')
   Drop View vETLDimDoctors;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimDoctors')
   Drop Procedure pETLFillDimDoctors;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimClinics')
   Drop View vETLDimClinics;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimClinics')
   Drop Procedure pETLFillDimClinics;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimShifts')
   Drop View vETLDimShifts;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimShifts')
   Drop Procedure pETLFillDimShifts;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimProcedures')
   Drop View vETLDimProcedures;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimProcedures')
   Drop Procedure pETLFillDimProcedures;
go
	If Exists(Select * from Sys.objects where Name = 'vETLFactDoctorShifts')
   Drop View vETLFactDoctorShifts;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillFactDoctorShifts')
   Drop Procedure pETLFillFactDoctorShifts;
go
	If Exists(Select * from Sys.objects where Name = 'vETLFactVisits')
   Drop View vETLFactVisits;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillFactVisits')
   Drop Procedure pETLFillFactVisits;
go
	If Exists(Select * from Sys.objects where Name = 'pETLAddForeignKeyConstraints')
   Drop Procedure pETLAddForeignKeyConstraints;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimPatients')
   Drop View vETLDimPatients;
go
	If Exists(Select * from Sys.objects where Name = 'pETLSyncDimPatients')
   Drop Procedure pETLSyncDimPatients;
go


Create Procedure pETLDropForeignKeyConstraints
/* Author: <FeiyangYu>
** Desc: Removed FKs before truncation of the tables
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	Alter Table DWClinicReportData.dbo.FactDoctorShifts
	  Drop Constraint fkFactDoctorShiftsToDimDates; 
	Alter Table DWClinicReportData.dbo.FactDoctorShifts
	  Drop Constraint fkFactDoctorShiftsToDimClinics; 
	Alter Table DWClinicReportData.dbo.FactDoctorShifts
	  Drop Constraint fkFactDoctorShiftsToDimShifts; 
	Alter Table DWClinicReportData.dbo.FactDoctorShifts
	  Drop Constraint fkFactDoctorShiftsToDimDoctors; 


	Alter Table DWClinicReportData.dbo.FactVisits
	  Drop Constraint fkFactVisitsToDimDates; 
	Alter Table DWClinicReportData.dbo.FactVisits
	  Drop Constraint fkFactVisitsToDimClinics; 
	Alter Table DWClinicReportData.dbo.FactVisits
	  Drop Constraint fkFactVisitsToDimPatients; 
	Alter Table DWClinicReportData.dbo.FactVisits
	  Drop Constraint fkFactVisitsToDimDoctors; 
	Alter Table DWClinicReportData.dbo.FactVisits
	  Drop Constraint fkFactVisitsToDimProcedures; 
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go


Create Procedure pETLTruncateTables
/* Author: <FeiyangYu>
** Desc: Flushes all date from the tables
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	Truncate Table [DWClinicReportData].dbo.FactVisits;
	Truncate Table [DWClinicReportData].dbo.FactDoctorShifts;
    Truncate Table [DWClinicReportData].dbo.DimDates;
	Truncate Table [DWClinicReportData].dbo.DimClinics;
	Truncate Table [DWClinicReportData].dbo.DimDoctors;
	Truncate Table [DWClinicReportData].dbo.DimShifts;
	Truncate Table [DWClinicReportData].dbo.DimProcedures;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go


Create View vETLDimDoctors
/* Author: <FeiyangYu>
** Desc: Extracts and transforms data for DimDoctors
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
As
  SELECT
    [DoctorID] = d.DoctorID,
	[DoctorFullName] = CAST(d.FirstName + ' ' + d.LastName as nvarchar(200)),
	[DoctorEmailAddress] = d.EmailAddress,
	[DoctorCity] = d.City,
	[DoctorState] = d.State,
	[DoctorZip] = d.Zip
  FROM [DoctorsSchedules].dbo.Doctors as d
go


Create Procedure pETLFillDimDoctors
/* Author: <FeiyangYu>
** Desc: Inserts data into DimDoctors
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	IF ((Select Count(*) From DimDoctors) = 0)
     Begin
      INSERT INTO [DWClinicReportData].dbo.DimDoctors
      ([DoctorID],[DoctorFullName],[DoctorEmailAddress],[DoctorCity],[DoctorState],[DoctorZip])
      SELECT
        [DoctorID]
       ,[DoctorFullName]
       ,[DoctorEmailAddress]
       ,[DoctorCity]
       ,[DoctorState] 
       ,[DoctorZip] 
      FROM vETLDimDoctors
	End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go


Create View vETLDimClinics
/* Author: <FeiyangYu>
** Desc: Extracts and transforms data for DimClinics
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
As
  SELECT
    [ClinicID] = c.ClinicID,
	[ClinicName] = c.ClinicName,
	[ClinicCity] = c.City,
	[ClinicState] = c.[State],
	[ClinicZip] = c.Zip
  FROM [DoctorsSchedules].dbo.Clinics as c
go


Create Procedure pETLFillDimClinics
/* Author: <FeiyangYu>
** Desc: Inserts data into DimClinics
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	IF ((Select Count(*) From DimClinics) = 0)
     Begin
      INSERT INTO [DWClinicReportData].dbo.DimClinics
      ([ClinicID],[ClinicName],[ClinicCity],[ClinicState],[ClinicZip])
      SELECT
        [ClinicID]
       ,[ClinicName]
       ,[ClinicCity]
       ,[ClinicState]
       ,[ClinicZip] 
      FROM vETLDimClinics
	End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go



Create View vETLDimProcedures
/* Author: <FeiyangYu>
** Desc: Extracts and transforms data for DimProcedures
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
As
  SELECT
    [ProcedureID] = p.ID,
	[ProcedureName] = p.[Name],
	[ProcedureDesc] = p.[Desc],
	[ProcedureCharge] = p.Charge
  FROM [Patients].dbo.Procedures as p
go

Create Procedure pETLFillDimProcedures
/* Author: <FeiyangYu>
** Desc: Inserts data into DimProcedures
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	IF ((Select Count(*) From DimProcedures) = 0)
     Begin
      INSERT INTO [DWClinicReportData].dbo.DimProcedures
      ([ProcedureID],[ProcedureName],[ProcedureDesc],[ProcedureCharge])
      SELECT
        [ProcedureID]
       ,[ProcedureName]
       ,[ProcedureDesc]
       ,[ProcedureCharge]
      FROM vETLDimProcedures
	End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go



Create View vETLDimShifts
/* Author: <FeiyangYu>
** Desc: Extracts and transforms data for DimShifts
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
As
  SELECT
    [ShiftID] = s.ShiftID,
	[ShiftStart] = Case s.ShiftStart 
	When '01:00:00' then '13:00:00' 
	When '09:00:00' then '09:00:00' 
	When '21:00:00' then '21:00:00' End,
	[ShiftEnd] = Case s.ShiftEnd 
	When '05:00:00' then '17:00:00' 
	When '21:00:00' then '21:00:00'
	When '09:00:00' then '09:00:00' End
  FROM [DoctorsSchedules].dbo.Shifts as s
go

Create Procedure pETLFillDimShifts
/* Author: <FeiyangYu>
** Desc: Inserts data into DimShifts
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	IF ((Select Count(*) From DimShifts) = 0)
     Begin
      INSERT INTO [DWClinicReportData].dbo.DimShifts
      ([ShiftID],[ShiftStart],[ShiftEnd])
      SELECT
        [ShiftID]
       ,[ShiftStart]
       ,[ShiftEnd]
      FROM vETLDimShifts
	End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go


Create Procedure pETLFillDimDates
/* Author: <FeiyangYu>
** Desc: Inserts data into DimDates
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
      Declare @StartDate datetime = '01/01/2005'
      Declare @EndDate datetime = '01/02/2010' 
      Declare @DateInProcess datetime  = @StartDate
      -- Loop through the dates until you reach the end date
      While @DateInProcess <= @EndDate
       Begin
       -- Add a row into the date dimension table for this date
	   SET IDENTITY_INSERT [DWClinicReportData].dbo.DimDates ON
       Insert Into DimDates 
       ( [DateKey], [FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName] )
       Values ( 
         Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) -- [DateKey]
		,@DateInProcess -- [FullDate]
        ,DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(50), @DateInProcess, 110) -- [FullDateName]  
        ,Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  -- [MonthID]
        ,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
        ,Year(@DateInProcess) -- [YearID] 
        ,Cast(Year(@DateInProcess) as nVarchar(50)) -- [YearName] 
        )  
       -- Add a day and loop again
       Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
       End
	  SET IDENTITY_INSERT [DWClinicReportData].dbo.DimDates OFF
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go






Create View vETLDimPatients
/* Author: <FeiyangYu>
** Desc: Extracts and transforms data for DimPatients
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
As
  SELECT
    [PatientID] = p.ID,
	[PatientFullName] = CAST(p.FName + ' ' + p.LName as varchar(100)),
	[PatientCity] = CAST(p.City as varchar(100)),
	[PatientState] = CAST(p.[State] as varchar(100)),
	[PatientZipCode] = p.ZipCode
  FROM [Patients].dbo.Patients as p
go



Create Procedure pETLSyncDimPatients
/* Author: <FeiyangYu>
** Desc: Inserts data into DimPatients
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --

    -- 1) For UPDATE: Change the EndDate and IsCurrent on any added rows 
	With ChangedPatients
		As(
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode]  From vETLDimPatients
			Except
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From DimPatients
    )UPDATE [DWClinicReportData].dbo.DimPatients
      SET EndDate = Cast(GetDate() as date)
         ,IsCurrent = 0
       WHERE PatientID IN (Select PatientID From ChangedPatients)
    ;

    -- 2) For INSERT or UPDATES: Add new rows to the table
	With AddedORChangedPatients
		As(
            Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From vETLDimPatients
			Except
			Select PatientID, [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From DimPatients
		)INSERT INTO [DWClinicReportData].dbo.DimPatients
      ([PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode],[StartDate],[EndDate],[IsCurrent])
      SELECT
        [PatientID]
       ,[PatientFullName]
       ,[PatientCity]
       ,[PatientState]
	   ,[PatientZipCode]
       ,[StartDate] = Cast(GetDate() as date)
       ,[EndDate] = Null 
       ,[IsCurrent] = 1
      FROM vETLDimPatients
      WHERE PatientID IN (Select PatientID From AddedORChangedPatients)
    ;

    -- 3) For Delete: Change the IsCurrent status to zero
 With DeletedPatients
		As(
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From DimPatients
 			Except            			
            Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From vETLDimPatients
   	)UPDATE [DWClinicReportData].dbo.DimPatients
      SET EndDate = Cast(GetDate() as date)
         ,IsCurrent = 0
       WHERE PatientID IN (Select PatientID From DeletedPatients)
   ;

   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go



Create View vETLFactDoctorShifts
/* Author: <FeiyangYu>
** Desc: Extracts and transforms data for vETLFactDoctorShifts
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
As
  SELECT
    [DoctorShiftID] = ds.DoctorsShiftID
   ,[ShiftDateKey] = d.DateKey
   ,[ClinicKey] = c.ClinicKey
   ,[ShiftKey] = s.ShiftKey
   ,[DoctorKey] = doc.DoctorKey
   ,[HoursWorked] = DATEDIFF(hh, s.ShiftStart, s.ShiftEnd)
  FROM [DoctorsSchedules].dbo.DoctorShifts as ds
  JOIN [DWClinicReportData].dbo.DimDates as d
   ON Cast(Convert(nVarchar(50), ds.ShiftDate, 112) as int) = d.DateKey
  JOIN [DWClinicReportData].dbo.DimClinics as c
   ON c.ClinicID = ds.ClinicID
  JOIN [DWClinicReportData].dbo.DimDoctors as doc
   ON doc.DoctorID = ds.DoctorID
  JOIN [DWClinicReportData].dbo.DimShifts as s
   ON s.ShiftID = ds.ShiftID
go


Create Procedure pETLFillFactDoctorShifts
/* Author: <FeiyangYu>
** Desc: Inserts data into FactDoctorShifts
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
   IF ((Select Count(*) From FactDoctorShifts) = 0)
     Begin
      INSERT INTO [DWClinicReportData].dbo.FactDoctorShifts
      ([DoctorsShiftID], [ShiftDateKey], [ClinicKey], 
	   [ShiftKey], [DoctorKey], [HoursWorked])
      SELECT
        [DoctorShiftID]
	   ,[ShiftDateKey]
	   ,[ClinicKey]
	   ,[ShiftKey]
       ,[DoctorKey]
       ,[HoursWorked]
      FROM vETLFactDoctorShifts
	End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go


Create View vETLFactVisits
/* Author: <FeiyangYu>
** Desc: Extracts and transforms data for vETLFactVisits
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
As
  SELECT
	[VisitKey] = v.ID
   ,[DateKey] = d.DateKey
   ,[ClinicKey] = c.ClinicKey
   ,[PatientKey] = p.PatientKey
   ,[DoctorKey] =  ISNULL(doc.DoctorKey, 0)
   ,[ProcedureKey] = pro.ProcedureKey
   ,[ProcedureVistCharge] = pro.ProcedureCharge
   FROM [Patients].dbo.Visits as v
   JOIN [DWClinicReportData].dbo.DimDates as d
   ON cast(format(v.[Date],'yyyyMMdd') as int) = d.DateKey
  JOIN [DWClinicReportData].dbo.DimClinics as c
   ON c.ClinicID * 100 = v.Clinic
   FULL OUTER JOIN [DWClinicReportData].dbo.DimDoctors as doc
   ON doc.DoctorID = v.Doctor
  JOIN [DWClinicReportData].dbo.DimPatients as p
   ON v.Patient = p.PatientID
  JOIN [DWClinicReportData].DBO.DimProcedures  as pro
   ON v.[Procedure] = pro.ProcedureID
go


Create Procedure pETLFillFactVisits
/* Author: <FeiyangYu>
** Desc: Inserts data into FactVisits
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	 IF ((Select Count(*) From FactVisits) = 0)
     Begin
      INSERT INTO [DWClinicReportData].dbo.FactVisits
      ([VisitKey], [DateKey], [ClinicKey], [PatientKey], 
	   [DoctorKey], [ProcedureKey], [ProcedureVistCharge])
      SELECT
	    [VisitKey]
       ,[DateKey]
	   ,[ClinicKey]
	   ,[PatientKey]
	   ,[DoctorKey]
       ,[ProcedureKey]
       ,[ProcedureVistCharge]
      FROM vETLFactVisits
	End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go


Create Procedure pETLAddForeignKeyConstraints
/* Author: <FeiyangYu>
** Desc: Removed FKs before truncation of the tables
** Change Log: When,Who,What
** 2019-08-26,<FeiyangYu>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    ALTER TABLE DWClinicReportData.dbo.FactDoctorShifts
      ADD CONSTRAINT fkFactDoctorShiftsToDimDates
      FOREIGN KEY (ShiftDateKey) REFERENCES DimDates(DateKey);
	ALTER TABLE DWClinicReportData.dbo.FactDoctorShifts
      ADD CONSTRAINT fkFactDoctorShiftsToDimClinics
      FOREIGN KEY (ClinicKey) REFERENCES DimClinics(ClinicKey);
	ALTER TABLE DWClinicReportData.dbo.FactDoctorShifts
      ADD CONSTRAINT fkFactDoctorShiftsToDimShifts
      FOREIGN KEY (ShiftKey) REFERENCES DimShifts(ShiftKey);
	ALTER TABLE DWClinicReportData.dbo.FactDoctorShifts
      ADD CONSTRAINT fkFactDoctorShiftsToDimDoctors
      FOREIGN KEY (DoctorKey) REFERENCES DimDoctors(DoctorKey);
	ALTER TABLE DWClinicReportData.dbo.FactVisits
      ADD CONSTRAINT fkFactVisitsToDimDates
      FOREIGN KEY (DateKey) REFERENCES  DimDates(DateKey);
	ALTER TABLE DWClinicReportData.dbo.FactVisits
      ADD CONSTRAINT fkFactVisitsToDimClinics
      FOREIGN KEY (ClinicKey) REFERENCES DimClinics(ClinicKey);
	ALTER TABLE DWClinicReportData.dbo.FactVisits
      ADD CONSTRAINT fkFactVisitsToDimPatients
      FOREIGN KEY (PatientKey) REFERENCES DimPatients(PatientKey);
	ALTER TABLE DWClinicReportData.dbo.FactVisits WITH NOCHECK
      ADD CONSTRAINT fkFactVisitsToDimDoctors
      FOREIGN KEY (DoctorKey) REFERENCES DimDoctors(DoctorKey) ;
	ALTER TABLE DWClinicReportData.dbo.FactVisits
      ADD CONSTRAINT fkFactVisitsToDimProcedures
      FOREIGN KEY (ProcedureKey) REFERENCES DimProcedures(ProcedureKey);
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go


Declare @Status int = 0;
Exec @Status = pETLDropForeignKeyConstraints;
Select [Object] = 'pETLDropForeignKeyConstraints', [Status] = @Status;

Exec @Status = pETLTruncateTables;
Select [Object] = 'pETLTruncateTables', [Status] = @Status;

Exec @Status = pETLFillDimDoctors;
Select [Object] = 'pETLFillDimDoctors', [Status] = @Status;
Exec @Status = pETLFillDimClinics;
Select [Object] = 'pETLFillDimClinics', [Status] = @Status;
Exec @Status = pETLFillDimProcedures;
Select [Object] = 'pETLFillDimProcedures', [Status] = @Status;
Exec @Status = pETLFillDimShifts;
Select [Object] = 'pETLFillDimShifts', [Status] = @Status;
Exec @Status = pETLFillDimDates;
Select [Object] = 'pETLFillDimDates', [Status] = @Status;
Exec @Status = pETLSyncDimPatients;
Select [Object] = 'pETLSyncDimPatients', [Status] = @Status;
Exec @Status = pETLFillFactDoctorShifts;
Select [Object] = 'pETLFillFactDoctorShifts', [Status] = @Status;
Exec @Status = pETLFillFactVisits;
Select [Object] = 'pETLFillFactVisits', [Status] = @Status;
Exec @Status = pETLAddForeignKeyConstraints;
Select [Object] = 'pETLAddForeignKeyConstraints', [Status] = @Status;
go

--select * from DimDoctors
--Select * from DimClinics
--Select * from DimProcedures
--Select * from DimShifts
--Select * from DimDates
--Select * from DimPatients
--Select * from FactDoctorShifts
--Select * from FactVisits