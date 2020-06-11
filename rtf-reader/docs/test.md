1、20191115 hive测试
add jar /home/dd_edw_rtf/original-RTFReaderV2-2.0-SNAPSHOT.jar;
select * from fdm.fdm_pek_orders_rtf_rcnt_local limit 3;

select * from fdm.fdm_pek_orders_rtf_rcnt_local where to_date(createdate)=sysdate() limit 5;

select count(1) from fdm.fdm_pek_orders_rtf_rcnt_local where to_date(createdate)=sysdate() ;
3748892