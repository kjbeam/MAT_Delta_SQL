create or replace PROCEDURE         SP_SALESFORCEDELTA
AS
    -- Variable that holds the count of total records for this run. Used in the MAT_DELTA_SF_RUNLOG Table.
    P_SF_STAGING_COUNT NUMBER;
    -- Variable that holds last runtime from the MAT_DELTA_SF_RUNLOG Table.
    P_SF_LASTRUNDATETIME DATE;

BEGIN

-- Retrieve the last runtime from the runlog.
SELECT RUNTIME INTO P_SF_LASTRUNDATETIME FROM MAT_DELTA_SF_RUNLOG WHERE ID IN (SELECT MAX(ID) FROM MAT_DELTA_SF_RUNLOG);

--Step 1 : Truncate Delta Staging Table
EXECUTE IMMEDIATE 'TRUNCATE TABLE MAT_DELTA_SF_STAGING';
COMMIT;

--Step 2  : Check for Delta Logic (SEASONS.TRACK_DELTA = 'Y' and SEASONS.INITIAL_DATA_LOADED = 'Y').
--Step 2A : Delta - Adoptions (Generic is populated, Color Marketing Name is populated 
--                  and one of the adoption date fields are populated)
--          Modified Date of the field must fall between the last runlog datetime and the current run datetime
INSERT /*+ append  */ INTO MAT_DELTA_SF_STAGING
(
  MAG_ID,  MAG_NAME,  MAGTEMPLATES_ID,  MAGTEMPLATES_NAME,
  ROW_ID,  BRAND_ID,  BRAND_NAME,  SEASONS_ID,
  SEASON_YEAR,  SEASON_NAME, SEASON_SHORTNAME,  SF_FIELDNAME,  FIELDVALUE,
  SORT_ORDER,SEGMENT_COUNT,CREATE_DATE,MODIFIED_DATE
)
WITH ADOPTIONS_GENERIC_NUM_CHECK AS -- Pull magtemplates_id and row_id where there is a generic number
(
    SELECT A.magtemplates_id, A.row_id 
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
    WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'Y')
    AND (A.FIELDS_ID = 6 AND a.fieldvalue IS NOT NULL)
),
ADOPTIONS_CMN_FIELDS_CHECK AS -- Pull magtemplates_id and row_id where color marketing name is populated
(
    SELECT A.magtemplates_id, A.row_id, A.fieldvalue as ZZ_PLMID, B.fieldvalue as ZZ_CHOICEID, C.SEASON_YEAR as FLEX_YEAR, C.SEASON_NAME as FLEX_SEASON
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
    INNER JOIN MAT B ON B.magtemplates_id = A.MAGTEMPLATES_ID AND B.row_id = A.row_id
    WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'Y')
      AND (A.FIELDS_ID = 1 AND A.fieldvalue IS NOT NULL)
      AND (B.FIELDS_ID = 2 AND B.fieldvalue IS NOT NULL)
),
ADOPTIONS_CMN_CHECK AS
(
    SELECT A.magtemplates_id, A.row_id
    FROM ADOPTIONS_CMN_FIELDS_CHECK A
    INNER JOIN mpa_master.flex_stylecolors B 
        ON A.ZZ_PLMID = B.ZZ_PLMID 
        AND A.ZZ_CHOICEID = B.ZZ_CHOICEID
        AND A.FLEX_YEAR = B.FLEX_YEAR
        AND A.FLEX_SEASON = B.FLEX_SEASON
),
ADOPTIONS_DATE_CHECK AS -- Pull magtemplates_id and row_id where at least one of the following dates exist
                        -- Stores (184), Digital (185), International (186) Adoption Time Stamp
(
    SELECT A.magtemplates_id, A.row_id 
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
    WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'Y')
    AND (A.FIELDS_ID in (184,185,186) AND A.fieldvalue IS NOT NULL)
    GROUP BY A.magtemplates_id, A.row_id
),
ADOPTIONS_RESULTS AS -- Pull magtemplates_id and row_id where there is a generic number and a color marketing name
(
    SELECT A.magtemplates_id, A.row_id 
    FROM ADOPTIONS_GENERIC_NUM_CHECK A
    INNER JOIN ADOPTIONS_CMN_CHECK B ON A.MAGTEMPLATES_ID = B.MAGTEMPLATES_ID AND A.row_id = B.row_id
    INNER JOIN ADOPTIONS_DATE_CHECK C ON A.MAGTEMPLATES_ID = C.MAGTEMPLATES_ID AND A.row_id = C.row_id
)
SELECT C.MAG_ID,C.MAG_NAME, A.MAGTEMPLATES_ID, C.TEMPLATES_NAME, 
    A.ROW_ID, C.BRAND_ID, C.BRAND_NAME, C.SEASONS_ID, 
    C.SEASON_YEAR, C.SEASON_NAME, C.SEASON_SHORTNAME, B.SALESFORCE_NAME, A.FIELDVALUE, 
    B.SORT_ORDER,A.MAGTEMPLATES_ID,A.CREATE_DATE,A.MODIFIED_DATE 
FROM MAT A 
INNER JOIN MAT_DELTA_SF_FIELDS B ON A.FIELDS_ID = B.FIELDS_ID
INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
INNER JOIN ADOPTIONS_RESULTS D ON d.magtemplates_id = A.MAGTEMPLATES_ID and d.row_id = a.row_id 
WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'Y')
AND A.MODIFIED_DATE > P_SF_LASTRUNDATETIME;
COMMIT;

--Step 2B : Delta - Generics (Generic is populated, Color Marketing Name is NOT populated)
--                  Pull Style Related Fields Only
--                  Modified Date of the field must fall between the last runlog datetime and the current run datetime
INSERT /*+ append  */ INTO MAT_DELTA_SF_STAGING
(
  MAG_ID,  MAG_NAME,  MAGTEMPLATES_ID,  MAGTEMPLATES_NAME,
  ROW_ID,  BRAND_ID,  BRAND_NAME,  SEASONS_ID,
  SEASON_YEAR,  SEASON_NAME, SEASON_SHORTNAME,  SF_FIELDNAME,  FIELDVALUE,
  SORT_ORDER,SEGMENT_COUNT,CREATE_DATE,MODIFIED_DATE
)
WITH GENERICS_GENERIC_NUM_CHECK AS -- Pull magtemplates_id and row_id where there is a generic number
(
    SELECT A.magtemplates_id, A.row_id 
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
    WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'Y')
    AND (A.FIELDS_ID = 6 AND a.fieldvalue IS NOT NULL)
),
GENERICS_CMN_FIELDS_CHECK AS -- Pull magtemplates_id and row_id where color marketing name is populated
                             -- We will LEFT JOIN with the magtemplates/rows that have generic number
                             -- in GENERIC_RESULTS so that we only have magtemplates/rows that have a 
                             -- generic number populated but do not have a color marketing name.
(
    SELECT A.magtemplates_id, A.row_id, A.fieldvalue as ZZ_PLMID, B.fieldvalue as ZZ_CHOICEID, C.SEASON_YEAR as FLEX_YEAR, C.SEASON_NAME as FLEX_SEASON
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
    INNER JOIN MAT B ON B.magtemplates_id = A.MAGTEMPLATES_ID AND B.row_id = A.row_id
    WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'Y')
      AND (A.FIELDS_ID = 1 AND A.fieldvalue IS NOT NULL)
      AND (B.FIELDS_ID = 2 AND B.fieldvalue IS NOT NULL)
),
GENERICS_CMN_CHECK AS
(
    SELECT A.magtemplates_id, A.row_id
    FROM GENERICS_CMN_FIELDS_CHECK A
    INNER JOIN mpa_master.flex_stylecolors B 
        ON A.ZZ_PLMID = B.ZZ_PLMID 
        AND A.ZZ_CHOICEID = B.ZZ_CHOICEID
        AND A.FLEX_YEAR = B.FLEX_YEAR
        AND A.FLEX_SEASON = B.FLEX_SEASON
),
GENERICS_RESULTS AS -- Pull magtemplates_id and row_id where there is a generic number and no color marketing name
(
    SELECT A.magtemplates_id, A.row_id
    FROM GENERICS_GENERIC_NUM_CHECK A
    LEFT JOIN GENERICS_CMN_CHECK B ON A.MAGTEMPLATES_ID = B.MAGTEMPLATES_ID AND A.ROW_ID = B.ROW_ID
    WHERE (B.MAGTEMPLATES_ID IS NULL AND B.ROW_ID IS NULL)
)
SELECT C.MAG_ID,C.MAG_NAME, A.MAGTEMPLATES_ID, C.TEMPLATES_NAME, 
    A.ROW_ID, C.BRAND_ID, C.BRAND_NAME, C.SEASONS_ID, 
    C.SEASON_YEAR, C.SEASON_NAME, C.SEASON_SHORTNAME, B.SALESFORCE_NAME, A.FIELDVALUE, 
    B.SORT_ORDER,A.MAGTEMPLATES_ID,A.CREATE_DATE,A.MODIFIED_DATE 
FROM MAT A 
INNER JOIN MAT_DELTA_SF_FIELDS B ON A.FIELDS_ID = B.FIELDS_ID AND B.STYLE_LEVEL = 'Y'
INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
INNER JOIN GENERICS_RESULTS D ON d.magtemplates_id = A.MAGTEMPLATES_ID and d.row_id = a.row_id 
WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'Y')
AND A.MODIFIED_DATE > P_SF_LASTRUNDATETIME;
COMMIT;

--Step 3  : Check for Initial Load Logic (SEASONS.TRACK_DELTA = 'Y' and SEASONS.INITIAL_DATA_LOADED = 'N')
--Step 3A : Initial Load - Adoptions (Generic is populated, Color Marketing Name is populated 
--                         and one of the adoption date fields are populated)
INSERT /*+ append  */ INTO MAT_DELTA_SF_STAGING
(
  MAG_ID,  MAG_NAME,  MAGTEMPLATES_ID,  MAGTEMPLATES_NAME,
  ROW_ID,  BRAND_ID,  BRAND_NAME,  SEASONS_ID,
  SEASON_YEAR,  SEASON_NAME, SEASON_SHORTNAME, SF_FIELDNAME,  FIELDVALUE,
  SORT_ORDER,SEGMENT_COUNT,CREATE_DATE,MODIFIED_DATE
)
WITH ADOPTIONS_GENERIC_NUM_CHECK AS -- Pull magtemplates_id and row_id where there is a generic number
(
    SELECT A.magtemplates_id, A.row_id 
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
    WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'N')
    AND (A.FIELDS_ID = 6 AND a.fieldvalue IS NOT NULL)
),
ADOPTIONS_CMN_CHECK AS -- Pull magtemplates_id and row_id where there is a color marketing name
(
    SELECT A.magtemplates_id, A.row_id 
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
    WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'N')
    AND (A.FIELDS_ID = 404 AND A.fieldvalue IS NOT NULL)
    
), 
ADOPTIONS_DATE_CHECK AS -- Pull magtemplates_id and row_id where at least one of the following dates exist
                        -- Stores (184), Digital (185), International (186) Adoption Time Stamp
                        -- Pull all fields
(
    SELECT A.magtemplates_id, A.row_id 
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
    WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'N')
    AND (A.FIELDS_ID in (184,185,186) AND A.fieldvalue IS NOT NULL)
    GROUP BY A.magtemplates_id, A.row_id
),
ADOPTIONS_RESULTS AS -- Pull magtemplates_id and row_id where there is a generic number and a color marketing name
(
    SELECT A.magtemplates_id, A.row_id 
    FROM ADOPTIONS_GENERIC_NUM_CHECK A
    INNER JOIN ADOPTIONS_CMN_CHECK B ON A.MAGTEMPLATES_ID = B.MAGTEMPLATES_ID AND A.row_id = B.row_id
    INNER JOIN ADOPTIONS_DATE_CHECK C ON A.MAGTEMPLATES_ID = C.MAGTEMPLATES_ID AND A.row_id = C.row_id
)
SELECT C.MAG_ID,C.MAG_NAME, A.MAGTEMPLATES_ID, C.TEMPLATES_NAME, 
    A.ROW_ID, C.BRAND_ID, C.BRAND_NAME, C.SEASONS_ID, 
    C.SEASON_YEAR, C.SEASON_NAME, C.SEASON_SHORTNAME, B.SALESFORCE_NAME, A.FIELDVALUE, 
    B.SORT_ORDER,A.MAGTEMPLATES_ID,A.CREATE_DATE,A.MODIFIED_DATE 
FROM MAT A 
INNER JOIN MAT_DELTA_SF_FIELDS B ON A.FIELDS_ID = B.FIELDS_ID
INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
INNER JOIN ADOPTIONS_RESULTS D ON d.magtemplates_id = A.MAGTEMPLATES_ID and d.row_id = a.row_id 
WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'N');
COMMIT;

--Step 3B : Delta - Generics (Generic is populated, Color Marketing Name is NOT populated)
--                  Pull Style Related Fields Only
INSERT /*+ append  */ INTO MAT_DELTA_SF_STAGING
(
  MAG_ID,  MAG_NAME,  MAGTEMPLATES_ID,  MAGTEMPLATES_NAME,
  ROW_ID,  BRAND_ID,  BRAND_NAME,  SEASONS_ID,
  SEASON_YEAR,  SEASON_NAME, SEASON_SHORTNAME,  SF_FIELDNAME,  FIELDVALUE,
  SORT_ORDER,SEGMENT_COUNT,CREATE_DATE,MODIFIED_DATE
)
WITH GENERICS_GENERIC_NUM_CHECK AS -- Pull magtemplates_id and row_id where there is a generic number
(
    SELECT A.magtemplates_id, A.row_id 
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
    WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'N')
    AND (A.FIELDS_ID = 6 AND a.fieldvalue IS NOT NULL)
),
GENERICS_CMN_FIELDS_CHECK AS -- Pull magtemplates_id and row_id where color marketing name is populated
                             -- We will LEFT JOIN with the magtemplates/rows that have generic number
                             -- in GENERIC_RESULTS so that we only have magtemplates/rows that have a 
                             -- generic number populated but do not have a color marketing name.
(
    SELECT A.magtemplates_id, A.row_id, A.fieldvalue as ZZ_PLMID, B.fieldvalue as ZZ_CHOICEID, C.SEASON_YEAR as FLEX_YEAR, C.SEASON_NAME as FLEX_SEASON
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
    INNER JOIN MAT B ON B.magtemplates_id = A.MAGTEMPLATES_ID AND B.row_id = A.row_id
    WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'N')
      AND (A.FIELDS_ID = 1 AND A.fieldvalue IS NOT NULL)
      AND (B.FIELDS_ID = 2 AND B.fieldvalue IS NOT NULL)
),
GENERICS_CMN_CHECK AS
(
    SELECT A.magtemplates_id, A.row_id
    FROM GENERICS_CMN_FIELDS_CHECK A
    INNER JOIN mpa_master.flex_stylecolors B 
        ON A.ZZ_PLMID = B.ZZ_PLMID 
        AND A.ZZ_CHOICEID = B.ZZ_CHOICEID
        AND A.FLEX_YEAR = B.FLEX_YEAR
        AND A.FLEX_SEASON = B.FLEX_SEASON
),
GENERICS_RESULTS AS -- Pull magtemplates_id and row_id where there is a generic number and no color marketing name
(
    SELECT A.magtemplates_id, A.row_id
    FROM GENERICS_GENERIC_NUM_CHECK A
    LEFT JOIN GENERICS_CMN_CHECK B ON A.MAGTEMPLATES_ID = B.MAGTEMPLATES_ID AND A.ROW_ID = B.ROW_ID
    WHERE (B.MAGTEMPLATES_ID IS NULL AND B.ROW_ID IS NULL)
)
SELECT C.MAG_ID,C.MAG_NAME, A.MAGTEMPLATES_ID, C.TEMPLATES_NAME, 
    A.ROW_ID, C.BRAND_ID, C.BRAND_NAME, C.SEASONS_ID, 
    C.SEASON_YEAR, C.SEASON_NAME, C.SEASON_SHORTNAME, B.SALESFORCE_NAME, A.FIELDVALUE, 
    B.SORT_ORDER,A.MAGTEMPLATES_ID,A.CREATE_DATE,A.MODIFIED_DATE 
FROM MAT A 
INNER JOIN MAT_DELTA_SF_FIELDS B ON A.FIELDS_ID = B.FIELDS_ID AND B.STYLE_LEVEL = 'Y'
INNER JOIN MDIS_MAGTEMPLATES C ON C.ID = A.MAGTEMPLATES_ID
INNER JOIN GENERICS_RESULTS D ON d.magtemplates_id = A.MAGTEMPLATES_ID and d.row_id = a.row_id 
WHERE C.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'N');
COMMIT;

--Step 4 : Insert Fiber Content fields using Planning Choice
INSERT /*+ append  */ INTO MAT_DELTA_SF_STAGING (
  MAG_ID,  MAG_NAME,  MAGTEMPLATES_ID,  MAGTEMPLATES_NAME,
  ROW_ID,  BRAND_ID,  BRAND_NAME,  SEASONS_ID,
  SEASON_YEAR,  SEASON_NAME, SEASON_SHORTNAME, SF_FIELDNAME,FIELDVALUE,SORT_ORDER,SEGMENT_COUNT,CREATE_DATE,MODIFIED_DATE
)
WITH MATARTICLE AS
(
    SELECT /*+ parallel(B,4) */ MAX(B.MATNR) ARTICLE, A.FIELDVALUE ZZ_CHOICEID,
        A.MAG_ID,A.MAG_NAME,A.MAGTEMPLATES_ID,A.MAGTEMPLATES_NAME,A.ROW_ID,A.BRAND_ID,A.BRAND_NAME,A.SEASONS_ID,A.SEASON_YEAR,
        A.SEASON_NAME,A.SEASON_SHORTNAME,A.SORT_ORDER,A.CREATE_DATE,A.MODIFIED_DATE
    FROM MAT_DELTA_SF_STAGING A INNER JOIN MPA_MASTER.SAP_MARA B 
    ON A.FIELDVALUE = TRIM(B.ZZ_CHOICEID)
    WHERE A.SF_FIELDNAME = 'Planning_Choice__c' AND A.FIELDVALUE IS NOT NULL
    GROUP BY    A.MAG_ID,A.MAG_NAME,A.MAGTEMPLATES_ID,A.MAGTEMPLATES_NAME,A.ROW_ID,A.BRAND_ID,A.BRAND_NAME,A.SEASONS_ID,
                A.SEASON_YEAR,A.SEASON_NAME,A.SEASON_SHORTNAME,A.FIELDVALUE,A.SORT_ORDER,A.CREATE_DATE,A.MODIFIED_DATE
),
DISTARTICLE AS
(
    SELECT DISTINCT ARTICLE FROM MATARTICLE
),
RESULTS AS
(
    SELECT /*+ MATERIALIZE*/ * FROM MPA_MASTER.SAP_ESTDF ESTDF WHERE TEXTCAT = 'Z_INT_FB_P'
),
FIBER_DATE AS 
(
    SELECT * FROM 
    (
        SELECT /*+ parallel(ESTVA,8) */ ESTMJ.UPDDAT, DBMS_LOB.SUBSTR(ESTDF.HEADER, 1000, 1 ) AS FIELD_VALUE, ESTMJ.MATNR AS ARTICLE,
        DENSE_RANK() OVER (PARTITION BY ESTMJ.MATNR ORDER BY ROWNUM) DENSE_RANK
        FROM RESULTS ESTDF
        INNER JOIN MPA_MASTER.SAP_AUSP_EHS AUSP ON SUBSTR(AUSP.OBJEK,0,20) = ESTDF.RECNMST
        INNER JOIN MPA_MASTER.SAP_ESTVA ESTVA ON SUBSTR(AUSP.OBJEK,0,20) = ESTVA.RECN
        INNER JOIN MPA_MASTER.SAP_ESTVH ESTVH ON ESTVH.RECN = ESTVA.RECNTVH
        INNER JOIN MPA_MASTER.SAP_ESTMJ ESTMJ ON ESTMJ.RECNROOT = ESTVH.RECNROOT 
        WHERE TEXTCAT = 'Z_INT_FB_P'
        AND AUSP.ATINN = 'Z_INT_LANG' 
        AND AUSP.ATWRT = 'EN'
        AND EXISTS (SELECT ARTICLE FROM DISTARTICLE DA WHERE DA.ARTICLE = ESTMJ.MATNR)
        ORDER BY  ESTMJ.MATNR
    ) WHERE DENSE_RANK = 1
)
SELECT MATD.MAG_ID,  MATD.MAG_NAME,  MATD.MAGTEMPLATES_ID,  MATD.MAGTEMPLATES_NAME,
  MATD.ROW_ID,  MATD.BRAND_ID,  MATD.BRAND_NAME,  MATD.SEASONS_ID,
  MATD.SEASON_YEAR,  MATD.SEASON_NAME, MATD.SEASON_SHORTNAME,'Fiber_Content__c', FIBER.FIELD_VALUE,
  MATD.SORT_ORDER, MATD.MAGTEMPLATES_ID,MATD.CREATE_DATE,MATD.MODIFIED_DATE
FROM MATARTICLE MATD INNER JOIN FIBER_DATE FIBER ON MATD.ARTICLE = FIBER.ARTICLE;
COMMIT;

--Step 5 : Insert Master Style Description from the reporting view based on Planning Choice
INSERT /*+ append  */ INTO MAT_DELTA_SF_STAGING (
  MAG_ID,  MAG_NAME,  MAGTEMPLATES_ID,  MAGTEMPLATES_NAME,
  ROW_ID,  BRAND_ID,  BRAND_NAME,  SEASONS_ID,
  SEASON_YEAR,  SEASON_NAME, SEASON_SHORTNAME, SF_FIELDNAME,FIELDVALUE,SORT_ORDER,SEGMENT_COUNT,CREATE_DATE,MODIFIED_DATE
)
SELECT STG.MAG_ID,  STG.MAG_NAME,  STG.MAGTEMPLATES_ID,  STG.MAGTEMPLATES_NAME,
  STG.ROW_ID,  STG.BRAND_ID,  STG.BRAND_NAME,  STG.SEASONS_ID,
  STG.SEASON_YEAR,  STG.SEASON_NAME, STG.SEASON_SHORTNAME,'Master_Style_Desc__c', RPT.masterstyle_desc,
  STG.SORT_ORDER, STG.MAGTEMPLATES_ID,STG.CREATE_DATE,STG.MODIFIED_DATE
FROM mat_delta_sf_staging STG, rpt_mat_reporting RPT
WHERE STG.sf_fieldname = 'Planning_Choice__c'
AND RPT.masterstyle_desc IS NOT NULL
AND STG.fieldvalue = trim(RPT.planning_choice)
AND RPT.FLEX_YEAR = STG.SEASON_YEAR
AND RPT.FLEX_SEASON = STG.SEASON_NAME;
COMMIT;

--Step 6 : Insert Generic Description from the reporting view based on Planning Choice
INSERT /*+ append  */ INTO MAT_DELTA_SF_STAGING (
  MAG_ID,  MAG_NAME,  MAGTEMPLATES_ID,  MAGTEMPLATES_NAME,
  ROW_ID,  BRAND_ID,  BRAND_NAME,  SEASONS_ID,
  SEASON_YEAR,  SEASON_NAME, SEASON_SHORTNAME, SF_FIELDNAME,FIELDVALUE,SORT_ORDER,SEGMENT_COUNT,CREATE_DATE,MODIFIED_DATE
)
SELECT STG.MAG_ID,  STG.MAG_NAME,  STG.MAGTEMPLATES_ID,  STG.MAGTEMPLATES_NAME,
  STG.ROW_ID,  STG.BRAND_ID,  STG.BRAND_NAME,  STG.SEASONS_ID,
  STG.SEASON_YEAR,  STG.SEASON_NAME, STG.SEASON_SHORTNAME,'Generic_Description__c', RPT.generic_desc,
  STG.SORT_ORDER, STG.MAGTEMPLATES_ID,STG.CREATE_DATE,STG.MODIFIED_DATE
FROM mat_delta_sf_staging STG, rpt_mat_reporting RPT
WHERE STG.sf_fieldname = 'Planning_Choice__c'
AND RPT.generic_desc IS NOT NULL
AND STG.fieldvalue = trim(RPT.planning_choice)
AND RPT.FLEX_YEAR = STG.SEASON_YEAR
AND RPT.FLEX_SEASON = STG.SEASON_NAME;
COMMIT;

--Step 7 : If this is an initial load for a season (SEASONS.TRACK_DELTA = 'Y' and SEASONS.INITIAL_DATA_LOADED = 'N')
--         Set the NEW_ADD_DELETE flag in the Staging table for all Planning Choice records (Adoptions) to 'N' (New) 
UPDATE
(SELECT A.new_add_delete
FROM mat_delta_sf_staging A
INNER JOIN seasons B
ON A.season_year = B.year AND A.season_name = B.name
WHERE A.sf_fieldname = 'Planning_Choice__c'
AND B.track_delta = 'Y' AND B.initial_data_loaded = 'N') C
SET C.new_add_delete = 'N';
COMMIT;

-- Insert records into the MAT_DELTA_SF_ADOPTIONS table
INSERT /*+ append  */ INTO MAT_DELTA_SF_ADOPTIONS (
  MAGTEMPLATES_ID,
  PLANNING_CHOICE
)
SELECT a.magtemplates_id, a.fieldvalue
FROM mat_delta_sf_staging A
INNER JOIN seasons B
ON A.season_year = B.year AND A.season_name = B.name
WHERE A.sf_fieldname = 'Planning_Choice__c'
AND b.track_delta = 'Y' AND b.initial_data_loaded = 'N';
COMMIT;

--Step 8 : If a mat_delta_sf_staging.magtemplates_id does not exist in mat_delta_sf_adoptions
--         and mat_delta_sf_staging.new_add_delete flag has not been populated
--         then we know this is a new MAT workbook, not an initial load (because the new_add_delete flag
--         was populated for initial loading above), and that all of the planning choices associated
--         with this MAT should be set to new.
UPDATE
(SELECT A.new_add_delete
FROM mat_delta_sf_staging A
LEFT JOIN mat_delta_sf_adoptions B
ON A.magtemplates_id = B.magtemplates_id
WHERE B.magtemplates_id IS NULL
AND A.sf_fieldname = 'Planning_Choice__c'
AND A.new_add_delete IS NULL) C
SET C.new_add_delete = 'N';
COMMIT;

-- Insert records into the MAT_DELTA_SF_ADOPTIONS table
INSERT /*+ append  */ INTO MAT_DELTA_SF_ADOPTIONS (
  MAGTEMPLATES_ID,
  PLANNING_CHOICE
)
SELECT a.magtemplates_id, a.fieldvalue
FROM mat_delta_sf_staging A
LEFT JOIN mat_delta_sf_adoptions B
ON A.magtemplates_id = B.magtemplates_id
WHERE B.magtemplates_id IS NULL
AND A.sf_fieldname = 'Planning_Choice__c'
AND A.new_add_delete IS NULL;
COMMIT;

--Step 9 : If the mat_delta_sf_staging.magtemplates_id/Planning Choice combination does not exist
--         in mat_delta_sf_adoptions and mat_delta_sf_staging.new_add_delete flag has not been populated
--         (because the new_add_delete flag was populated for initial loading or because it was a 
--          new MAT workbook during the delta process)
--         Planning Choice records should be set to 'Add'
UPDATE
(SELECT A.new_add_delete
FROM mat_delta_sf_staging A
LEFT JOIN mat_delta_sf_adoptions B
ON A.magtemplates_id = B.magtemplates_id AND A.fieldvalue = B.planning_choice
WHERE B.magtemplates_id IS NULL
AND A.sf_fieldname = 'Planning_Choice__c'
AND A.new_add_delete IS NULL) C
SET C.new_add_delete = 'A';
COMMIT;

-- Insert records into the MAT_DELTA_SF_ADOPTIONS table
INSERT /*+ append  */ INTO MAT_DELTA_SF_ADOPTIONS (
  MAGTEMPLATES_ID,
  PLANNING_CHOICE
)
SELECT A.new_add_delete
FROM mat_delta_sf_staging A
LEFT JOIN mat_delta_sf_adoptions B
ON A.magtemplates_id = B.magtemplates_id AND A.fieldvalue = B.planning_choice
WHERE B.magtemplates_id IS NULL
AND A.sf_fieldname = 'Planning_Choice__c'
AND A.new_add_delete IS NULL;
COMMIT;

--Step 10 : If the mat_delta_sf_adoptions.magtemplates_id/Planning Choice combination does not exist
--          in the main MAT table, this means that the row that contains this planning choice has
--          been deleted.

-- Insert a planning choice records into mat_delta_sf_staging designating that they are deleted
-- (mat_delta_sf_staging.new_add_delete = 'D')
INSERT /*+ append  */ INTO MAT_DELTA_SF_STAGING (
  MAG_ID,  MAG_NAME,  MAGTEMPLATES_ID,  MAGTEMPLATES_NAME,
  ROW_ID,  BRAND_ID,  BRAND_NAME,  SEASONS_ID, SEASON_YEAR,  SEASON_NAME, 
  SEASON_SHORTNAME, SF_FIELDNAME,FIELDVALUE,SORT_ORDER,SEGMENT_COUNT,
  CREATE_DATE,MODIFIED_DATE, NEW_ADD_DELETE
)
WITH MAT_MT_PC AS -- Pull magtemplates_id and planning choice records from MAT where we are tracking the season
(
    SELECT A.magtemplates_id AS MAT_MT_ID, A.fieldvalue as MAT_PLANNING_CHOICE 
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES B ON B.ID = A.MAGTEMPLATES_ID
    WHERE B.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'Y')
    AND A.FIELDS_ID = 3
),
MT_PC_IN_ADOPTIONS_NOT_MAT AS -- Select mat_delta_sf_adoptions records that are not in the MAT table
(
    SELECT *
    FROM mat_delta_sf_adoptions A
    LEFT JOIN MAT_MT_PC B
    ON A.magtemplates_id = B.MAT_MT_ID AND A.planning_choice = B.MAT_PLANNING_CHOICE
    WHERE B.MAT_MT_ID IS NULL and B.MAT_PLANNING_CHOICE IS NULL
)
SELECT 999999 AS MAG_ID, 'MAG_DELETE' AS MAG_NAME, MAGTEMPLATES_ID, 
        'MAGTEMPLATES_DELETE' AS MAGTEMPLATES_NAME,  999999 AS ROW_ID,  
        999999 AS BRAND_ID, 'BRAND_DELETE' AS BRAND_NAME, 999999 AS SEASONS_ID, 
        '9999' AS SEASON_YEAR, 'SEASON_DELETE' AS SEASON_NAME, 
        '9999' AS SEASON_SHORTNAME, 'Planning_Choice__c' AS FIELDNAME, PLANNING_CHOICE AS FIELDVALUE, 
        137 AS SORT_ORDER, MAGTEMPLATES_ID AS SEGMENT_COUNT,
        TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS') AS CREATE_DATE, 
        TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS') AS MODIFIED_DATE, 'D'
FROM MT_PC_IN_ADOPTIONS_NOT_MAT
COMMIT;

--Delete the records from the mat_delta_sf_adoptions table because they no longer exist in MAT
DELETE FROM mat_delta_sf_adoptions 
WHERE magtemplates_id in
(WITH MAT_MT_PC AS -- Pull magtemplates_id and planning choice records from MAT where we are tracking the season
(
    SELECT A.magtemplates_id AS MAT_MT_ID, A.fieldvalue as MAT_PLANNING_CHOICE 
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES B ON B.ID = A.MAGTEMPLATES_ID
    WHERE B.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'Y')
    AND A.FIELDS_ID = 3
),
MT_PC_IN_ADOPTIONS_NOT_MAT AS -- Select mat_delta_sf_adoptions records that are not in the MAT table
(
    SELECT *
    FROM mat_delta_sf_adoptions A
    LEFT JOIN MAT_MT_PC B
    ON A.magtemplates_id = B.MAT_MT_ID AND A.planning_choice = B.MAT_PLANNING_CHOICE
    WHERE B.MAT_MT_ID IS NULL and B.MAT_PLANNING_CHOICE IS NULL
)
SELECT magtemplates_id FROM MT_PC_IN_ADOPTIONS_NOT_MAT)
AND
planning_choice in 
(WITH MAT_MT_PC AS -- Pull magtemplates_id and planning choice records from MAT where we are tracking the season
(
    SELECT A.magtemplates_id AS MAT_MT_ID, A.fieldvalue as MAT_PLANNING_CHOICE 
    FROM MAT A
    INNER JOIN MDIS_MAGTEMPLATES B ON B.ID = A.MAGTEMPLATES_ID
    WHERE B.SEASONS_ID IN (SELECT ID FROM SEASONS WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'Y')
    AND A.FIELDS_ID = 3
),
MT_PC_IN_ADOPTIONS_NOT_MAT AS -- Select mat_delta_sf_adoptions records that are not in the MAT table
(
    SELECT *
    FROM mat_delta_sf_adoptions A
    LEFT JOIN MAT_MT_PC B
    ON A.magtemplates_id = B.MAT_MT_ID AND A.planning_choice = B.MAT_PLANNING_CHOICE
    WHERE B.MAT_MT_ID IS NULL and B.MAT_PLANNING_CHOICE IS NULL
)
SELECT planning_choice FROM MT_PC_IN_ADOPTIONS_NOT_MAT);
COMMIT;

--Step 8 : After an inital load update SEASONS.INITIAL_DATA_LOADED from 'N' to 'Y'.
UPDATE SEASONS SET INITIAL_DATA_LOADED = 'Y' WHERE TRACK_DELTA = 'Y' AND INITIAL_DATA_LOADED = 'N';
COMMIT;

--Step 9 : Retrieve the total records count from the staging table.
SELECT COUNT(*) INTO P_SF_STAGING_COUNT FROM MAT_DELTA_SF_STAGING;

--Step 10 : Insert a record into the runlog with information regarding this run.
INSERT INTO MAT_DELTA_SF_RUNLOG (RUNTIME, RECORDS)
SELECT SYSDATE, P_SF_STAGING_COUNT FROM DUAL;
COMMIT;

END SP_SALESFORCEDELTA;