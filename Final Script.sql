-- ===================================================================================
-- Step 1: Create the database
-- ===================================================================================
USE master;
GO

IF DB_ID('Healthcare_DW') IS NOT NULL
BEGIN
    ALTER DATABASE Healthcare_DW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Healthcare_DW;
END
GO

CREATE DATABASE Healthcare_DW;
GO

USE Healthcare_DW;
GO

-- ===================================================================================
-- Step 2: Create the Staging Table 
-- ===================================================================================
DROP TABLE IF EXISTS Staging_Admissions;
CREATE TABLE Staging_Admissions (
    SourceAdmissionID VARCHAR(64) NOT NULL PRIMARY KEY, 
    Name VARCHAR(255),
    Age INT,
    Gender VARCHAR(50),
    Blood_Type VARCHAR(10),
    Medical_Condition VARCHAR(255),
    Date_of_Admission DATE,
    Doctor VARCHAR(255),
    Hospital VARCHAR(255),
    Insurance_Provider VARCHAR(255),
    Billing_Amount DECIMAL(12,2),
    Room_Number VARCHAR(50),
    Admission_Type VARCHAR(255),
    Discharge_Date DATE,
    Medication VARCHAR(255),
    Test_Results VARCHAR(255),
    Hospital_Latitude DECIMAL(12,6),
    Hospital_Longitude DECIMAL(12,6)
);
GO

-- ===================================================================================
-- Step 3: Create Dimension Tables
-- ===================================================================================
DROP TABLE IF EXISTS DimPatients;
CREATE TABLE DimPatients (
  PatientID         INT PRIMARY KEY IDENTITY(1,1),
  PatientSourceID   VARCHAR(255) NOT NULL UNIQUE, 
  PatientName       VARCHAR(255) NULL,
  Gender            VARCHAR(20) NOT NULL,
  BloodType         VARCHAR(10) NOT NULL
);

DROP TABLE IF EXISTS DimDoctors;
CREATE TABLE DimDoctors (
    DoctorID INT PRIMARY KEY IDENTITY(1,1),
    DoctorName VARCHAR(255) UNIQUE
);

-- EDITED: DimHospitals now includes ONLY Lat and Long (grain remains Hospital level)
DROP TABLE IF EXISTS DimHospitals;
CREATE TABLE DimHospitals (
    HospitalID INT PRIMARY KEY IDENTITY(1,1),
    HospitalName VARCHAR(255) UNIQUE,
    Hospital_Latitude DECIMAL(12,6),
    Hospital_Longitude DECIMAL(12,6)
);

DROP TABLE IF EXISTS DimInsurance;
CREATE TABLE DimInsurance (
    InsuranceID INT PRIMARY KEY IDENTITY(1,1),
    InsuranceProvider VARCHAR(255) UNIQUE
);
GO

-- ===================================================================================
-- Step 4: Insert "Unknown" Members for Data Quality
-- ===================================================================================
SET IDENTITY_INSERT DimPatients ON;
INSERT INTO DimPatients (PatientID, PatientSourceID, PatientName, Gender, BloodType)
VALUES (-1, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown');
SET IDENTITY_INSERT DimPatients OFF;

SET IDENTITY_INSERT DimDoctors ON;
INSERT INTO DimDoctors (DoctorID, DoctorName)
VALUES (-1, 'Unknown');
SET IDENTITY_INSERT DimDoctors OFF;

-- EDITED: Added default values for Lat/Long
SET IDENTITY_INSERT DimHospitals ON;
INSERT INTO DimHospitals (HospitalID, HospitalName, Hospital_Latitude, Hospital_Longitude)
VALUES (-1, 'Unknown', 0.0, 0.0);
SET IDENTITY_INSERT DimHospitals OFF;

SET IDENTITY_INSERT DimInsurance ON;
INSERT INTO DimInsurance (InsuranceID, InsuranceProvider)
VALUES (-1, 'Unknown');
SET IDENTITY_INSERT DimInsurance OFF;
GO

-- ===================================================================================
-- Step 5: Create the Fact Table 
-- ===================================================================================
DROP TABLE IF EXISTS FactAdmissions;
CREATE TABLE FactAdmissions (
  AdmissionID        INT PRIMARY KEY IDENTITY(1,1),
  SourceAdmissionID  VARCHAR(64) NOT NULL UNIQUE, 
  
  -- Foreign Keys
  PatientID          INT NOT NULL DEFAULT -1,
  DoctorID           INT NOT NULL DEFAULT -1,
  HospitalID         INT NOT NULL DEFAULT -1,
  InsuranceID        INT NOT NULL DEFAULT -1,

  -- EDITED: RoomNumber is back. Lat, Long, Year, Quarter, and Month are removed.
  AdmissionDate      DATE NOT NULL,
  DischargeDate      DATE NULL,
  AgeAtAdmission     INT,
  AdmissionType      VARCHAR(100),
  Medication         VARCHAR(100),
  TestResults        VARCHAR(100),
  RoomNumber         VARCHAR(50),
  BillingAmount      DECIMAL(12,2),
  MedicalCondition   VARCHAR(100),

  -- Kept LengthOfStay as it is a highly valuable pre-calculated fact metric
  LengthOfStay       AS (
      CASE 
          WHEN DATEDIFF(day, AdmissionDate, ISNULL(DischargeDate, AdmissionDate)) < 0 THEN 0 
          ELSE DATEDIFF(day, AdmissionDate, ISNULL(DischargeDate, AdmissionDate)) 
      END
  ) PERSISTED,

  CONSTRAINT fk_fact_patient   FOREIGN KEY (PatientID)  REFERENCES DimPatients (PatientID),
  CONSTRAINT fk_fact_doctor    FOREIGN KEY (DoctorID)   REFERENCES DimDoctors  (DoctorID),
  CONSTRAINT fk_fact_hospital  FOREIGN KEY (HospitalID) REFERENCES DimHospitals(HospitalID),
  CONSTRAINT fk_fact_insurance FOREIGN KEY (InsuranceID)REFERENCES DimInsurance(InsuranceID)
);
GO

-- ===================================================================================
-- Step 6: Create the Incremental ETL Stored Procedure
-- ===================================================================================
CREATE OR ALTER PROCEDURE sp_LoadDataWarehouse_Incremental
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        INSERT INTO DimDoctors (DoctorName)
        SELECT DISTINCT Doctor
        FROM Staging_Admissions
        WHERE Doctor IS NOT NULL AND Doctor != ''
          AND NOT EXISTS (SELECT 1 FROM DimDoctors d WHERE d.DoctorName = Staging_Admissions.Doctor);

        -- EDITED: Populate DimHospitals with Lat and Long (Grouped by HospitalName)
        INSERT INTO DimHospitals (HospitalName, Hospital_Latitude, Hospital_Longitude)
        SELECT DISTINCT Hospital, Hospital_Latitude, Hospital_Longitude
        FROM Staging_Admissions
        WHERE Hospital IS NOT NULL AND Hospital != ''
          AND NOT EXISTS (SELECT 1 FROM DimHospitals h WHERE h.HospitalName = Staging_Admissions.Hospital);

        INSERT INTO DimInsurance (InsuranceProvider)
        SELECT DISTINCT Insurance_Provider
        FROM Staging_Admissions
        WHERE Insurance_Provider IS NOT NULL AND Insurance_Provider != ''
          AND NOT EXISTS (SELECT 1 FROM DimInsurance i WHERE i.InsuranceProvider = Staging_Admissions.Insurance_Provider);

        WITH RankedPatients AS (
            SELECT
                Name, Gender, Blood_Type, Date_of_Admission,
                ROW_NUMBER() OVER(
                    PARTITION BY Name, Gender, Blood_Type 
                    ORDER BY Date_of_Admission DESC
                ) as rn
            FROM Staging_Admissions
            WHERE Name IS NOT NULL AND Gender IS NOT NULL AND Blood_Type IS NOT NULL
        )
        MERGE DimPatients AS target
        USING (
            SELECT 
                CONCAT_WS('_', Name, Gender, Blood_Type) AS PatientSourceID, 
                Name AS PatientName, 
                Gender, 
                Blood_Type AS BloodType
            FROM RankedPatients
            WHERE rn = 1
        ) AS source
        ON target.PatientSourceID = source.PatientSourceID
        
        WHEN MATCHED THEN 
            UPDATE SET 
                PatientName = source.PatientName,
                Gender = source.Gender,
                BloodType = source.BloodType
                
        WHEN NOT MATCHED THEN
            INSERT (PatientSourceID, PatientName, Gender, BloodType)
            VALUES (source.PatientSourceID, source.PatientName, source.Gender, source.BloodType);

        -- EDITED: Insert into FactAdmissions 
        INSERT INTO FactAdmissions (
            SourceAdmissionID, PatientID, DoctorID, HospitalID, InsuranceID,
            AdmissionDate, DischargeDate, AgeAtAdmission, AdmissionType, Medication, TestResults,
            RoomNumber, BillingAmount, MedicalCondition
        )
        SELECT
            s.SourceAdmissionID,
            ISNULL(dp.PatientID, -1)   AS PatientID,
            ISNULL(dd.DoctorID, -1)    AS DoctorID,
            ISNULL(dh.HospitalID, -1)  AS HospitalID,
            ISNULL(di.InsuranceID, -1) AS InsuranceID,
            
            s.Date_of_Admission,
            s.Discharge_Date,
            s.Age,
            s.Admission_Type,
            s.Medication,
            s.Test_Results,
            s.Room_Number,
            s.Billing_Amount,
            s.Medical_Condition
        FROM Staging_Admissions s
        LEFT JOIN DimPatients dp ON dp.PatientSourceID = CONCAT_WS('_', s.Name, s.Gender, s.Blood_Type)
        LEFT JOIN DimDoctors dd ON dd.DoctorName = s.Doctor
        -- EDITED: Join DimHospitals cleanly on HospitalName
        LEFT JOIN DimHospitals dh ON dh.HospitalName = s.Hospital 
        LEFT JOIN DimInsurance di ON di.InsuranceProvider = s.Insurance_Provider
        WHERE s.Date_of_Admission IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM FactAdmissions f WHERE f.SourceAdmissionID = s.SourceAdmissionID);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO