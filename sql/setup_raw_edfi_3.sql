SET NOCOUNT ON;

IF DB_ID('raw_edfi_3') IS NULL
BEGIN
    PRINT 'Creating database raw_edfi_3...';
    CREATE DATABASE raw_edfi_3;
END
GO

USE raw_edfi_3;
GO

IF SCHEMA_ID('dbo') IS NULL
BEGIN
    EXEC('CREATE SCHEMA dbo');
END
GO

-- helper: create table if not exists
DECLARE @tableName sysname;

DECLARE @tables TABLE(name sysname);

INSERT INTO @tables(name)
VALUES
    ('_deletes'),
    ('_descriptors'),
    ('assessments'),
    ('bell_schedules'),
    ('calendar_dates'),
    ('calendars'),
    ('class_periods'),
    ('cohorts'),
    ('contacts'),
    ('course_offerings'),
    ('course_transcripts'),
    ('courses'),
    ('credentials'),
    ('discipline_actions'),
    ('discipline_incidents'),
    ('education_organization_network_associations'),
    ('education_organization_networks'),
    ('education_service_centers'),
    ('grades'),
    ('grading_periods'),
    ('graduation_plans'),
    ('learning_standards'),
    ('local_education_agencies'),
    ('locations'),
    ('objective_assessments'),
    ('parents'),
    ('people'),
    ('post_secondary_institutions'),
    ('programs'),
    ('restraint_events'),
    ('schools'),
    ('sections'),
    ('sessions'),
    ('staff_education_organization_assignment_associations'),
    ('staff_education_organization_contact_associations'),
    ('staff_education_organization_employment_associations'),
    ('staff_school_associations'),
    ('staff_section_associations'),
    ('staffs'),
    ('state_education_agencies'),
    ('student_academic_records'),
    ('student_assessments'),
    ('student_cohort_associations'),
    ('student_contact_associations'),
    ('student_cte_program_associations'),
    ('student_discipline_incident_associations'),
    ('student_discipline_incident_behavior_associations'),
    ('student_discipline_incident_non_offender_associations'),
    ('student_education_organization_associations'),
    ('student_education_organization_responsibility_associations'),
    ('student_homeless_program_associations'),
    ('student_language_instruction_program_associations'),
    ('student_parent_associations'),
    ('student_program_associations'),
    ('student_school_associations'),
    ('student_school_attendance_events'),
    ('student_section_associations'),
    ('student_section_attendance_events'),
    ('student_special_education_program_associations'),
    ('student_title_i_part_a_program_associations'),
    ('students'),
    ('surveys'),
    ('survey_question_responses'),
    ('survey_questions'),
    ('survey_response_education_organization_target_associations'),
    ('survey_responses'),
    ('survey_section_responses'),
    ('survey_sections');

WHILE EXISTS (SELECT 1 FROM @tables)
BEGIN
    SELECT TOP 1 @tableName = name FROM @tables ORDER BY name;

    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' AND t.name = @tableName
    )
    BEGIN
        DECLARE @sql NVARCHAR(MAX) = N'CREATE TABLE [dbo].[' + @tableName + N'](
            [id] [int] IDENTITY(1,1) NOT NULL,
            [v] [varchar](max) NULL,
            [tenant_code] [varchar](20) NULL,
            [api_year] [varchar](9) NULL,
            [pull_timestamp] [datetime] NULL,
            [last_modified_timestamp] [datetime] NULL,
            [file_row_number] [int] NULL,
            [filename] [varchar](256) NULL,
            [is_deleted] [bit] NULL,
            [ods_version] [decimal](18, 0) NULL,
            [data_model_version] [decimal](18, 0) NULL,
            CONSTRAINT [PK_' + @tableName + N'] PRIMARY KEY CLUSTERED ([id] ASC)
            WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, 
                  ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
        ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]';

        PRINT 'Creating table dbo.' + @tableName + '...';
        EXEC sp_executesql @sql;
    END

    DELETE TOP (1) FROM @tables WHERE name = @tableName;
END

PRINT 'Setup complete.';
