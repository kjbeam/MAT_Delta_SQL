create or replace PROCEDURE SP_SF_THIRD_PARTY AS 
BEGIN

EXECUTE IMMEDIATE 'TRUNCATE TABLE MAT_DELTA_SF_THIRD_PARTY';
COMMIT;

INSERT /*+ append  */ INTO MAT_DELTA_SF_THIRD_PARTY
(
  ARTICLE, SUBBRAND, SUBBRAND_DESCR, PRODUCT_HIERARCHY, PRODUCT_SHORT_DESC, LEVEL1,
  LEVEL1_KNOWN_AS, LEVEL1_DESC, LEVEL2, LEVEL2_KNOWN_AS, LEVEL2_DESC, LEVEL3,
  LEVEL3_KNOWN_AS, LEVEL3_DESC, SIZE_US, COLLECTION, GROSS_WEIGHT, NET_WEIGHT, MARKET_PLACE, STYLE_LIFECYCLE,
  MAGTEMPLATES_ID, PLANNING_CHOICE, SEASON_SHORTNAME, DIGITAL_DROP_1, DIGITAL_DROP_2, DIGITAL_DROP_3,
  DIGITAL_DROP_4, DIGITAL_DROP_5, DIGITAL_ON_FLOOR, DIGITAL_OFF_FLOOR, DIG_ADOPT_TIME_STAMP, 
  STORE_ADOPT_TIME_STAMP, INT_ADOPT_TIME_STAMP

)
WITH ARTICLES AS 
(
    SELECT  EXP.MATNR, EXP.ZZ_CHOICEID, STAGING.SF_FIELDNAME, STAGING.MAGTEMPLATES_ID, STAGING.FIELDVALUE, STAGING.SEASON_SHORTNAME, STAGING.ROW_ID, CAWNT.ATWTB MARKETPLACE
    FROM    MPA_MAT.MAT_DELTA_SF_STAGING STAGING
            INNER JOIN MPA_MASTER.SAP_MARA EXP ON STAGING.FIELDVALUE = TRIM(EXP.ZZ_CHOICEID) AND STAGING.SF_FIELDNAME = 'Planning_Choice__c' 
            INNER JOIN MPA_MASTER.SAP_AUSP AUSP ON AUSP.OBJEK = EXP.MATNR AND AUSP.KLART = 'Z01' AND AUSP.ATINN = 'MARKETPLACE'
            INNER JOIN MPA_MASTER.SAP_CABN CAB ON AUSP.ATINN = CAB.ATNAM
            INNER JOIN  MPA_MASTER.SAP_CAWN CAWN ON CAWN.ATINN = CAB.ATINN AND LPAD(AUSP.ATZHL,4,'0') = CAWN.ATZHL
            INNER JOIN MPA_MASTER.SAP_CAWNT CAWNT ON CAWN.ATINN = CAWNT.ATINN AND CAWN.ATZHL = CAWNT.ATZHL
),
MATDATA AS -- MAT DATA PULLED BY PLANNING CHOICE
(
    SELECT 
    ART.MAGTEMPLATES_ID MAGTEMPLATES_ID, ART.ROW_ID ROW_ID, ART.MATNR, ART.ZZ_CHOICEID Planning_Choice__c, ART.SEASON_SHORTNAME SEASON_SHORTNAME, 
    ART.MARKETPLACE MARKETPLACE__c,
    DROP1.FIELDVALUE DIGITAL_DROP_1__c, DROP2.FIELDVALUE DIGITAL_DROP_2__c, DROP3.FIELDVALUE DIGITAL_DROP_3__c, 
    DROP4.FIELDVALUE DIGITAL_DROP_4__c, DROP5.FIELDVALUE DIGITAL_DROP_5__c, DIGI_OFF_FLOOR.FIELDVALUE DIGITAL_OFF_FLOOR__c,
    DIGI_ON_FLOOR.FIELDVALUE DIGITAL_ON_FLOOR__c, DIGITAL_STATUS.FIELDVALUE DIGITAL_STATUS__c,
    Digital_Adoption_Time_Stamp.FIELDVALUE Digital_Adoption_Time_Stamp__c,
    Store_Adoption_Time_Stamp.FIELDVALUE Store_Adoption_Time_Stamp__c,
    Int_Adoption_Time_Stamp.FIELDVALUE Int_Adoption_Time_Stamp__c   
    FROM  ARTICLES ART  
    LEFT JOIN MAT_DELTA_SF_STAGING DROP1 ON DROP1.MAGTEMPLATES_ID = ART.MAGTEMPLATES_ID AND DROP1.ROW_ID = ART.ROW_ID AND DROP1.SF_FIELDNAME = 'Digital_Drop_1__c'
    LEFT JOIN MAT_DELTA_SF_STAGING DROP2 ON DROP2.MAGTEMPLATES_ID = ART.MAGTEMPLATES_ID AND DROP2.ROW_ID = ART.ROW_ID AND DROP2.SF_FIELDNAME = 'Digital_Drop_2__c'
    LEFT JOIN MAT_DELTA_SF_STAGING DROP3 ON DROP3.MAGTEMPLATES_ID = ART.MAGTEMPLATES_ID AND DROP3.ROW_ID = ART.ROW_ID AND DROP3.SF_FIELDNAME = 'Digital_Drop_3__c'
    LEFT JOIN MAT_DELTA_SF_STAGING DROP4 ON DROP4.MAGTEMPLATES_ID = ART.MAGTEMPLATES_ID AND DROP4.ROW_ID = ART.ROW_ID AND DROP4.SF_FIELDNAME = 'Digital_Drop_4__c'
    LEFT JOIN MAT_DELTA_SF_STAGING DROP5 ON DROP5.MAGTEMPLATES_ID = ART.MAGTEMPLATES_ID AND DROP5.ROW_ID = ART.ROW_ID AND DROP5.SF_FIELDNAME = 'Digital_Drop_5__c'
    LEFT JOIN MAT_DELTA_SF_STAGING DIGI_OFF_FLOOR ON DIGI_OFF_FLOOR.MAGTEMPLATES_ID = ART.MAGTEMPLATES_ID AND DIGI_OFF_FLOOR.ROW_ID = ART.ROW_ID AND DIGI_OFF_FLOOR.SF_FIELDNAME = 'Digital_Off_Floor__c'
    LEFT JOIN MAT_DELTA_SF_STAGING DIGI_ON_FLOOR ON DIGI_ON_FLOOR.MAGTEMPLATES_ID = ART.MAGTEMPLATES_ID AND DIGI_ON_FLOOR.ROW_ID = ART.ROW_ID AND DIGI_ON_FLOOR.SF_FIELDNAME = 'Digital_On_Floor__c'
    LEFT JOIN MAT_DELTA_SF_STAGING DIGITAL_STATUS ON DIGITAL_STATUS.MAGTEMPLATES_ID = ART.MAGTEMPLATES_ID AND DIGITAL_STATUS.ROW_ID = ART.ROW_ID AND DIGITAL_STATUS.SF_FIELDNAME = 'Digital_Status__c'
    LEFT JOIN MAT_DELTA_SF_STAGING Digital_Adoption_Time_Stamp ON Digital_Adoption_Time_Stamp.MAGTEMPLATES_ID = ART.MAGTEMPLATES_ID AND Digital_Adoption_Time_Stamp.ROW_ID = ART.ROW_ID AND Digital_Adoption_Time_Stamp.SF_FIELDNAME = 'Digital_Adoption_Time_Stamp__c'
    LEFT JOIN MAT_DELTA_SF_STAGING Store_Adoption_Time_Stamp ON Store_Adoption_Time_Stamp.MAGTEMPLATES_ID = ART.MAGTEMPLATES_ID AND Store_Adoption_Time_Stamp.ROW_ID = ART.ROW_ID AND Store_Adoption_Time_Stamp.SF_FIELDNAME = 'Store_Adoption_Time_Stamp__c'
    LEFT JOIN MAT_DELTA_SF_STAGING Int_Adoption_Time_Stamp ON Int_Adoption_Time_Stamp.MAGTEMPLATES_ID = ART.MAGTEMPLATES_ID AND Int_Adoption_Time_Stamp.ROW_ID = ART.ROW_ID AND Int_Adoption_Time_Stamp.SF_FIELDNAME = 'Int_Adoption_Time_Stamp__c'
) 
SELECT LTRIM(MARA.MATNR,'0') ARTICLE__c, TO_CHAR(SUBRAND.ZZ_SUBRAND1) SUBRAND__c, TO_CHAR(SUBRAND.DESCR) SUBRAND_DESCR__c, 
       MARA.PRDHA PRODUCT_HIERARCHY__c, 
       MAMT.MAKTM PRODUCT_SHORT_DESC__c, TO_CHAR(HIER.ZZ_SECTOR) LEVEL1__c,'SECTOR' LEVEL1_KNOWN_AS__c, 
       TO_CHAR(HIER.ZZ_SEC_DESC) LEVEL1_DESC__c, TO_CHAR(HIER.ZZ_CATEGORY) LEVEL2__c,'CATEGORY' LEVEL2_KNOWN_AS__c, 
       TO_CHAR(HIER.ZZ_CAT_DESC) LEVEL2_DESC__c, MARA.MATKL LEVEL3,'CLASS' LEVEL3_KNOWN_AS__c, 
       TO_CHAR(HIER.ZZ_CLASS_DESC) LEVEL3_DESC__c, TO_CHAR(SZE.ZZ_SIZE1||SZE.ZZ_SIZE2) SIZE_US__c, ARTMAS.ZZ_COLLECT COLLECTION__c, 
       MARA.BRGEW  GROSS_WEIGHT__c,
       MARA.NTGEW NET_WEIGHT,CASE WHEN  MARKET.ATWRT = '0001' THEN 'YES' ELSE 'NO' END MARKET_PLACE__c,
       STYLE_LIFECYCLE.ATWRT STYLE_LIFECYCLE__c, MATDATA.MAGTEMPLATES_ID, MATDATA.Planning_Choice__c, MATDATA.SEASON_SHORTNAME,
       MATDATA.DIGITAL_DROP_1__C, MATDATA.DIGITAL_DROP_2__C, MATDATA.DIGITAL_DROP_3__C, MATDATA.DIGITAL_DROP_4__c,
       MATDATA.DIGITAL_DROP_5__C, MATDATA.DIGITAL_ON_FLOOR__C, MATDATA.DIGITAL_OFF_FLOOR__c,
       MATDATA.Digital_Adoption_Time_Stamp__c,
       MATDATA.Store_Adoption_Time_Stamp__c,
       MATDATA.Int_Adoption_Time_Stamp__c
FROM   MPA_MASTER.SAP_MARA MARA
       INNER JOIN   MPA_MASTER.SAP_MAMT MAMT ON MARA.ZZ_STYLE = MAMT.MATNR AND MAMT.SPRAS = 'E'
       INNER JOIN   MATDATA ON MATDATA.MATNR = MARA.MATNR
       LEFT JOIN MPA_MASTER.SAP_AUSP MARKET ON MARKET.OBJEK = MARA.MATNR AND MARKET.ATINN = 'MARKETPLACE'
       LEFT JOIN MPA_MASTER.SAP_AUSP STYLE_LIFECYCLE ON STYLE_LIFECYCLE.OBJEK = MARA.MATNR AND STYLE_LIFECYCLE.ATINN = 'STYLE_LIFECYCLE'
       LEFT JOIN MPA_MASTER.SAP_ZMD_HIERARCHY HIER ON MARA.ZZ_SUBCLASS1 = HIER.ZZ_SUBCLASS
       LEFT JOIN MPA_MASTER.SAP_ZZARTMAS ARTMAS ON MARA.MATNR = ARTMAS.MATNR 
       LEFT JOIN MPA_MASTER.SAP_AUSP AUSP ON MARA.MATNR = AUSP.OBJEK_TRIM AND AUSP.ATINN = 'SIZE' AND AUSP.KLART = '026'
       LEFT JOIN MPA_MASTER.SAP_ZZLBISIZE SZE ON AUSP.ATWRT = SZE.ZZ_SIZE
       LEFT JOIN MPA_MASTER.SAP_ZZSUBRANDT1  SUBRAND ON MARA.ZZ_SUBRAND1 = SUBRAND.ZZ_SUBRAND1
       LEFT JOIN MPA_MASTER.SAP_ZZLBICOLOR COLOR ON MARA.ZZ_CHOICE = COLOR.ZZ_COLOR
       LEFT JOIN MPA_MASTER.SAP_ZZSUBCLASS1 SUBCLASS ON MARA.ZZ_SUBCLASS1 = SUBCLASS.ZZ_SUBCLASS1
       LEFT JOIN MPA_MASTER.SAP_ZZMASTERSTYLET1 MASTERSTYLE ON TRIM(ARTMAS.ZZ_PTRNFMLY) = MASTERSTYLE.ZZ_PTRNFMLY;
COMMIT;
END SP_SF_THIRD_PARTY;