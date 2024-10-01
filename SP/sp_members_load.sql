CREATE OR REPLACE PROCEDURE public.sp_members_load()
 LANGUAGE plpgsql
AS $$
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

    DROP TABLE IF EXISTS members_dtls_staging;
    DROP TABLE IF EXISTS member_plans_staging;

    -- Insert only newly available data into staging table for member_dtls from streaming View based on the max offset for new/existing partitions
    CREATE TABLE members_dtls_staging AS
    SELECT
        coalesce(data."after_image"."MEMBERID",data."before_image"."MEMBERID")::INT AS memberid,
        coalesce(data."after_image"."MEMBER_NAME",data."before_image"."MEMBER_NAME")::VARCHAR(100) AS member_name,
        coalesce(data."after_image"."MEMBER_TYPE",data."before_image"."MEMBER_TYPE")::VARCHAR(50) AS member_type,
        coalesce(data."after_image"."AGE",data."before_image"."AGE")::INT AS age,
        coalesce(data."after_image"."GENDER",data."before_image"."GENDER")::CHAR(1) AS gender,
        coalesce(data."after_image"."EMAIL",data."before_image"."EMAIL")::VARCHAR(100) AS email,
        coalesce(data."after_image"."REGION",data."before_image"."REGION")::VARCHAR(50) AS region,
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

    -- Insert only newly available data into staging table for member_plans from streaming View based on the max offset for new/existing partitions
    CREATE TABLE member_plans_staging AS
    SELECT
        coalesce(data."after_image"."MemberID",data."before_image"."MemberID")::INT AS memberid,
        coalesce(data."after_image"."Medical_Plan",data."before_image"."Medical_Plan")::CHAR(1) AS medical_plan,
        coalesce(data."after_image"."Dental_Plan",data."before_image"."Dental_Plan")::CHAR(1) AS dental_plan,
        coalesce(data."after_image"."Vision_Plan",data."before_image"."Vision_Plan")::CHAR(1) AS vision_plan,
        coalesce(data."after_image"."Preventive_Immunization",data."before_image"."Preventive_Immunization")::VARCHAR(50) AS preventive_immunization,
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

      EXECUTE 'DELETE FROM members_dtls using members_dtls_staging WHERE members_dtls.memberid = members_dtls_staging.memberid';
      EXECUTE 'DELETE FROM member_plans using member_plans_staging WHERE member_plans.memberid = member_plans_staging.memberid';


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

    -- Insert the max kafka_offset for every loaded Kafka partition into the audit table
    INSERT INTO members_audit
    SELECT kafka_partition, MAX(kafka_offset)
    FROM (
        SELECT kafka_partition, kafka_offset
        FROM members_dtls_staging
        UNION ALL
        SELECT kafka_partition, kafka_offset
        FROM member_plans_staging
    )
    GROUP BY kafka_partition;

END;
$$
