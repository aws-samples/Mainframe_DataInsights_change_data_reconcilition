CREATE OR REPLACE PROCEDURE public.sp_members_load()
 LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_rows_affected INT;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;

    -- Start transaction
    BEGIN
        SET ENABLE_CASE_SENSITIVE_IDENTIFIER TO TRUE;
        REFRESH MATERIALIZED VIEW member_view;

        -- Create an audit table if not exists to keep track of Max Offset per Partition
        CREATE TABLE IF NOT EXISTS members_audit
        (
            kafka_partition BIGINT,
            kafka_offset BIGINT
        )
        SORTKEY(kafka_partition, kafka_offset);

        -- Log the start of the procedure
        INSERT INTO audit_log (operation, details) 
        VALUES ('sp_members_load', 'Procedure started');

        -- Drop and recreate staging tables
        DROP TABLE IF EXISTS members_dtls_staging;
        DROP TABLE IF EXISTS member_plans_staging;

        -- Insert only newly available data into staging table for member_dtls
        CREATE TABLE members_dtls_staging AS
        SELECT
            coalesce(data."after_image"."MEMBERID",data."before_image"."MEMBERID")::INT AS memberid,
            left(coalesce(data."after_image"."MEMBER_NAME",data."before_image"."MEMBER_NAME"), 100)::VARCHAR(100) AS member_name,
            left(coalesce(data."after_image"."MEMBER_TYPE",data."before_image"."MEMBER_TYPE"), 50)::VARCHAR(50) AS member_type,
            CASE 
                WHEN coalesce(data."after_image"."AGE",data."before_image"."AGE")::INT BETWEEN 0 AND 120 
                THEN coalesce(data."after_image"."AGE",data."before_image"."AGE")::INT 
                ELSE NULL 
            END AS age,
            CASE 
                WHEN coalesce(data."after_image"."GENDER",data."before_image"."GENDER") IN ('M', 'F', 'O') 
                THEN coalesce(data."after_image"."GENDER",data."before_image"."GENDER")::CHAR(1) 
                ELSE NULL 
            END AS gender,
            CASE 
                WHEN length(coalesce(data."after_image"."EMAIL",data."before_image"."EMAIL")) <= 100 
                     AND coalesce(data."after_image"."EMAIL",data."before_image"."EMAIL") ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
                THEN coalesce(data."after_image"."EMAIL",data."before_image"."EMAIL")::VARCHAR(100) 
                ELSE NULL 
            END AS email,
            left(coalesce(data."after_image"."REGION",data."before_image"."REGION"), 50)::VARCHAR(50) AS region,
            data."change_op"::char(1) AS change_op,
            s.kafka_partition::BIGINT,
            s.kafka_offset::BIGINT
        FROM member_view s
        LEFT JOIN (
            SELECT
                kafka_partition,
                MAX(kafka_offset) AS kafka_offset
            FROM members_audit
            GROUP BY kafka_partition
        ) AS m
        ON NVL(s.kafka_partition, 0) = NVL(m.kafka_partition, 0)
        WHERE
            m.kafka_offset IS NULL OR
            s.kafka_offset > m.kafka_offset
            AND s.data."alias" = 'MEMBERS_DTLS';

        -- Insert only newly available data into staging table for member_plans
        CREATE TABLE member_plans_staging AS
        SELECT
            coalesce(data."after_image"."MemberID",data."before_image"."MemberID")::INT AS memberid,
            CASE 
                WHEN coalesce(data."after_image"."Medical_Plan",data."before_image"."Medical_Plan") IN ('Y', 'N') 
                THEN coalesce(data."after_image"."Medical_Plan",data."before_image"."Medical_Plan")::CHAR(1) 
                ELSE NULL 
            END AS medical_plan,
            CASE 
                WHEN coalesce(data."after_image"."Dental_Plan",data."before_image"."Dental_Plan") IN ('Y', 'N') 
                THEN coalesce(data."after_image"."Dental_Plan",data."before_image"."Dental_Plan")::CHAR(1) 
                ELSE NULL 
            END AS dental_plan,
            CASE 
                WHEN coalesce(data."after_image"."Vision_Plan",data."before_image"."Vision_Plan") IN ('Y', 'N') 
                THEN coalesce(data."after_image"."Vision_Plan",data."before_image"."Vision_Plan")::CHAR(1) 
                ELSE NULL 
            END AS vision_plan,
            left(coalesce(data."after_image"."Preventive_Immunization",data."before_image"."Preventive_Immunization"), 50)::VARCHAR(50) AS preventive_immunization,
            data."change_op"::char(1) AS change_op,
            s.kafka_partition::BIGINT,
            s.kafka_offset::BIGINT
        FROM member_view s
        LEFT JOIN (
            SELECT
                kafka_partition,
                MAX(kafka_offset) AS kafka_offset
            FROM members_audit
            GROUP BY kafka_partition
        ) AS m
        ON NVL(s.kafka_partition, 0) = NVL(m.kafka_partition, 0)
        WHERE 
            m.kafka_offset IS NULL OR
            s.kafka_offset > m.kafka_offset
            AND s.data."alias" = 'MEMBER_PLANS';

        -- Delete records from target table that also exist in staging table (updated/deleted records)
        DELETE FROM members_dtls 
        USING members_dtls_staging 
        WHERE members_dtls.memberid = members_dtls_staging.memberid;

        DELETE FROM member_plans 
        USING member_plans_staging 
        WHERE member_plans.memberid = member_plans_staging.memberid;

        -- Insert data into member_dtls table
        INSERT INTO members_dtls
        SELECT
            memberid,
            member_name,
            member_type,
            age,
            gender,
            email,
            region
        FROM members_dtls_staging
        WHERE change_op <> 'D';

        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        INSERT INTO audit_log (operation, details) 
        VALUES ('sp_members_load', 'Inserted ' || v_rows_affected || ' rows into members_dtls');

        -- Insert data into member_plans table
        INSERT INTO member_plans
        SELECT
            memberid,
            medical_plan,
            dental_plan,
            vision_plan,
            preventive_immunization
        FROM member_plans_staging
        WHERE change_op <> 'D';

        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        INSERT INTO audit_log (operation, details) 
        VALUES ('sp_members_load', 'Inserted ' || v_rows_affected || ' rows into member_plans');

        -- Insert the max kafka_offset for every loaded Kafka partition into the audit table
        INSERT INTO members_audit
        SELECT kafka_partition, MAX(kafka_offset)
        FROM (
            SELECT kafka_partition, kafka_offset
            FROM members_dtls_staging
            UNION ALL
            SELECT kafka_partition, kafka_offset
            FROM member_plans_staging
        ) AS combined_staging
        GROUP BY kafka_partition;

        -- Log successful completion
        INSERT INTO audit_log (operation, details) 
        VALUES ('sp_members_load', 'Procedure completed successfully. Execution time: ' || 
                (EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::INT) || ' seconds');

        -- Commit the transaction
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            -- Rollback the transaction
            ROLLBACK;
            -- Log the error
            INSERT INTO audit_log (operation, details) 
            VALUES ('sp_members_load', 'Error occurred: ' || SQLERRM);
            -- Re-raise the exception
            RAISE;
    END;
END;
$$;

-- Set appropriate permissions--for Principle of Least privilege
GRANT EXECUTE ON PROCEDURE public.sp_members_load TO specific_role;
GRANT SELECT, INSERT, DELETE ON members_dtls, member_plans, members_audit TO specific_role;
