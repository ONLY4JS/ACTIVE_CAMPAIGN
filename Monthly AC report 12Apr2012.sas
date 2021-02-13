DM "Output; Clear; Log; Clear";

libname M_cos oracle user=Marquee_Bi
                     pw=ice
					 path=RBDW_P
					 SCHEMA=MARQUEE_COS;
run;

PROC IMPORT OUT=Schedule 
            DATAFILE= "C:\Documents and Settings\Zhen.Qin\Desktop\ss.xls" 
            DBMS=EXCEL REPLACE;
            RANGE="Month$"; 
            GETNAMES=YES;
            MIXED=NO;
            SCANTEXT=YES;
            USEDATE=YES;
            SCANTIME=YES;
RUN;

data date_select;
set schedule;
sd1=input(scan(new_user_period,1,'-'),anydtdte20.);
sd2=input(scan(new_user_period,2,'-'),anydtdte20.);
pd1=input(scan(new_user_period,1,'-'),anydtdte20.);
pd2=input(scan(new_user_period,2,'-'),anydtdte20.);
x=scan(criteria,1,'=');
new_pd1=intnx('day',input(scan(new_user_period,1,'-'),anydtdte20.),-1);
new_pd2=intnx('day',input(scan(new_user_period,2,'-'),anydtdte20.),1);
format sd1 sd2 date7. pd1 pd2 new_pd1 new_pd2 worddate12.;
keep reporting_week data_from criteria sd1 sd2 pd1 pd2 x new_pd1 new_pd2;
run;

proc sql noprint;
select sd1 ,
       sd2 ,
	   data_from,
       criteria,
	   x,
	   pd1 ,
	   pd2 ,
	   sd2 ,new_pd1,
	   new_pd2
into       : d1,
	       : d2,
		   : ds,
		   : criteria,
		   : process,
		   : dp1,
		   : dp2,
		   : ss_date,
		   : n_pd1,
		   : n_pd2
from date_select
where week(reporting_week)=week(date());
quit;

%put &d1;
%put &d2;
%put &ds;
%put &criteria;
%put &process;
%put &dp1;
%put &dp2;
%put &ss_date;
%put &n_pd1;
%put &n_pd2;

%let s=',';
%let pd1 = %scan("&dp1",1,"&s");
%let pd2 = %scan("&dp2",1,"&s");
%put &pd1;
%put &pd2;
%let _pd1=%scan("&n_pd1",1,"&s");
%let _pd2=%scan("&n_pd2",1,"&s");
%put &_pd1;
%put &_pd2;

proc sql;
create table data_&ss_date as
select subscriber_seq,
       product_code as product,
       campaign_number as campaign,
	   campaign_start_date as start,
	   campaign_rate_end_date as end,
	   year(datepart(campaign_rate_end_date)) as year,
	   month(datepart(campaign_rate_end_date)) as month
from m_cos.&ds(keep=product_code
                     campaign_number
				     campaign_start_date
					 campaign_rate_end_date
					 &process
                     subscriber_seq)
where campaign_number is not null
			and campaign_start_date is not null
					and campaign_rate_end_date is not null
							and campaign_rate_end_date >"&ss_date"D
									and  &criteria;
quit;

proc sql;
create table d01 as
select *
from data_&ss_date(drop=product)
order by subscriber_seq, start, campaign;
quit;

proc sort data=d01 out=d02 nodupkeys;
by subscriber_seq campaign ;
run;

PROC IMPORT OUT= WORK.ALICE_CAMP 
            DATAFILE= "\\DRCSOMP0463\Shared_Area\PubilcDrive\Zhen\Campai
gn Reporting Phase 2\Acquisition-Retention Campaig
ns Updated 20120403.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="'Retention-Acquisition-Upsell$'"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

proc sql;
create table ac as
select campaign_description as description,
	   acq_ret_upsell as camp_cate,
	   campaign_code as campaign 
from alice_camp;
quit;


proc sql;
create table d060 as
select a.*,
       b.campaign_description as description,
	   b.acq_ret_upsell as camp_cate
from d02 a left join alice_camp(where=(length(campaign_code)=3)) b
on  a.campaign=b.campaign_code;
quit;


proc sort data=d060 out=d064_;
by subscriber_seq start campaign;
run;

data d061;
set d064_(where=(campaign not in ('RIT','USA','SU9','U41','US0','USS','UR9')));
by subscriber_seq start campaign;
if "&d1"d<=datepart(start)<="&d2"d then cata=1;
cata2=1;
run;

************ TSE Data ***************;

PROC IMPORT OUT= WORK.M 
            DATAFILE= "\\DRCSOMP0463\Shared_Area\PubilcDrive\Zhen\Campai
gn Reporting Phase 2\Master Prod Hierarchy YMD 2012-04-03 V2.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="'4-Master Prod Hierarchy$'"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

proc sql;
create table master as
select f1 as lob,
       f6 as product length=4 format=$4. 
from m(firstobs=5);
quit;


proc sort data=data_&ss_date out=d329 noduprecs;
by _all_;
run;

proc sql;
create table d847 as
select a.subscriber_seq,
       a.campaign,
	   a.start,
	   a.end,
	   a.year,
	   a.month,
	   b.product,
	   b.lob
from d329 a inner join master b
on a.product=b.product;
quit;


proc sql;
create table d947 as
select a.*,
       b.description,
	   b.camp_cate
from d847  a inner join ac b
on a.campaign=b.campaign;
quit;

data _tve _int _rhp _shm;
set d947(drop=product);
if lob='TVE' then output _tve;
else if lob='INT' then output _int;
else if lob='RHP' then output _rhp;
else if lob='SHM' then output _shm;
run;



proc sort data=_tve out=tve noduprecs;
by _all_;
run;

proc sort data=tve out=d_tve;
by subscriber_seq campaign start;
run;

data dd_tve;
set d_tve;
by subscriber_seq campaign start;
if (first.subscriber_seq and first.campaign)
    or (not first.subscriber_seq and first.campaign);
run;
	

proc sql;
create table dtve as
select a.*,
       case
	   when b.subscriber_seq then 1
	   else 0
	   end as a 
from dd_tve a left join d061(keep=subscriber_seq campaign cata where=(cata=1)) b
on a.subscriber_seq=b.subscriber_seq and a.campaign=b.campaign;
quit;


proc sort data=dtve out=ddtve;
by subscriber_seq campaign start;
run;

data dddtve;
set ddtve;
by subscriber_seq campaign start;
if "&d1"d<=datepart(start)<="&d2"d and a=1 then cata=1;
cata2=1;
run;




proc sort data=_int out=int noduprecs;
by _all_;
run;

proc sort data=int out=d_int;
by subscriber_seq campaign start;
run;

data dd_int;
set d_int;
by subscriber_seq campaign start;
if (first.subscriber_seq and first.campaign)
    or (not first.subscriber_seq and first.campaign);
run;


proc sql;
create table dint as
select a.*,
       case
	   when b.subscriber_seq then 1
	   else 0
	   end as a 
from dd_int a left join d061(keep=subscriber_seq campaign cata where=(cata=1)) b
on a.subscriber_seq=b.subscriber_seq and a.campaign=b.campaign;
quit;


proc sort data=dint out=ddint;
by subscriber_seq campaign start;
run;

data dddint;
set ddint;
by subscriber_seq campaign start;
if "&d1"d<=datepart(start)<="&d2"d and a=1 then cata=1;
cata2=1;
run;


proc sort data=_rhp out=rhp noduprecs;
by _all_;
run;

proc sort data=rhp out=d_rhp;
by subscriber_seq campaign start;
run;

data dd_rhp;
set d_rhp;
by subscriber_seq campaign start;
if (first.subscriber_seq and first.campaign)
    or (not first.subscriber_seq and first.campaign);
run;
proc sql;
create table drhp as
select a.*,
       case
	   when b.subscriber_seq then 1
	   else 0
	   end as a 
from dd_rhp a left join d061(keep=subscriber_seq campaign cata where=(cata=1)) b
on a.subscriber_seq=b.subscriber_seq and a.campaign=b.campaign;
quit;

proc sort data=drhp out=ddrhp;
by subscriber_seq campaign start;
run;

data dddrhp;
set ddrhp;
by subscriber_seq campaign start;
if "&d1"d<=datepart(start)<="&d2"d and a=1 then cata=1;
cata2=1;
run;

proc sort data=_shm out=shm noduprecs;
by _all_;
run;

proc sort data=shm out=d_shm;
by subscriber_seq campaign start;
run;

data dd_shm;
set d_shm;
by subscriber_seq campaign start;
if (first.subscriber_seq and first.campaign)
    or (not first.subscriber_seq and first.campaign);
run;

proc sql;
create table dshm as
select a.*,
       case
	   when b.subscriber_seq then 1
	   else 0
	   end as a 
from dd_shm a left join d061 (keep=subscriber_seq campaign cata where=(cata=1)) b
on a.subscriber_seq=b.subscriber_seq and a.campaign=b.campaign ;
quit;

proc sort data=dshm out=ddshm;
by subscriber_seq campaign start;
run;

data dddshm;
set ddshm;
by subscriber_seq campaign start;
if "&d1"d<=datepart(start)<="&d2"d and a=1 then cata=1;
cata2=1;
run;

proc format;                
value  cb   1 ='JAN'
            2 ='FEB'
		    3 ='MAR'
		    4 ='APR'
	        5 ='MAY'
	        6 ='JUN'
	        7 ='JUL'
	        8 ='AUG'
	        9 ='SEP'
	       10 ='OCT'
	       11 ='NOV'
	       12 ='DEC'
;
run;

proc template;
define style Styles.ZQ02;
parent = styles.Printer;
replace fonts /
'TitleFont2' = ("tahoma",9pt,Bold)
'TitleFont' = ("tahoma",9pt,Bold)
'StrongFont' = ("tahoma",8pt,Bold)
'EmphasisFont' = ("tahoma",8pt, Bold)
'FixedEmphasisFont' = ("tahoma",8pt)
'FixedStrongFont' = ("tahoma",8pt,Bold)
'FixedHeadingFont' = ("tahoma",8pt,Bold)
'BatchFixedFont' = ("tahoma",8pt)
'FixedFont' = ("tahoma",8pt)
'headingEmphasisFont' = ("tahoma",8pt,Bold )
'headingFont' = ("tahoma",8pt,Bold)
'docFont' = ("tahoma",8pt);
replace Systemtitle from TitlesAndFooters /
just = L;
replace GraphFonts /
'GraphDataFont' = ("tahoma",8pt)
'GraphValueFont' = ("tahoma",8pt)
'GraphLabelFont' = ("tahoma",9pt,Bold)
'GraphFootnoteFont' = ("tahoma",9pt,Bold)
'GraphTitleFont' = ("tahoma",9pt,Bold);
replace color_list /
'link' = blue
'bgH' = white
'fg' = black
'bg' = _undef_;
replace Table from Output /
background = _undef_
frame = HSIDES
vjust = M
cellpadding = 4pt
cellspacing = 0.75pt
borderwidth = 0.75pt;
replace SystemFooter from TitlesAndFooters /
just = L
font = fonts('docFont');
replace GraphFonts from GraphFonts  /
;
end;
run;




******************************************************************************************
******************************************************************************************;

proc sql;
create table d07 as
select *    
from data_&ss_date
order by subscriber_seq, product,campaign;
quit;

proc sort data=d07 out=d08 nodupkey;
by subscriber_seq product campaign;
run;

data d09;
set d08(where=(campaign not in ('RIT','USA','SU9','UR9')));
by subscriber_seq product campaign;
if "&d1"d<=datepart(start)<="&d2"d then cata=1;
cata2=1;
run;


ods path(prepend) work.templat(update);

%inc "\\DRCSOMP0463\Shared_Area\PubilcDrive\Zhen\excltags_010411.tpl";



options nocenter;
ods listing close;
ods tagsets.excelxp file="d:\x\MAC FROM %sysfunc(strip(&pd1)) TO %sysfunc(strip(&pd2)).xls" style=styles.zq02
options(orientation='landscape'
       	embedded_titles='yes'
		center_vertical='yes'
		row_repeat='1-4'
		autofilter='1-3'
		frozen_headers='yes'
		autofit_height='yes'
	
        );
ods tagsets.excelxp options(sheet_name='By Customer');		
title1 "MONTHLY ACTIVE CAMPAIGN -- SNAPSHOT DATE @%sysfunc(strip(&dp2))";
options missing='0';
ods escapechar='*';

proc report data=d061 nowindows nocompletecols style(header)=[vjust=MIDDLE] missing;
column    camp_cate  
         ("CATEGORY" (camp_cate1))
         ("CAMPAIGN" (campaign)) 
         
         ("DESCRIPTION" (description))
/*		 ("  NEW USERS    *    (>%sysfunc(strip(&_pd1)), <%sysfunc(strip(&_pd2)))" c)*/
         ("CAMPAIGN START DATE * USING MIN START DATE* (>%sysfunc(strip(&_pd1)), <%sysfunc(strip(&_pd2)))" (cata))
		 ("CAMPAIGN RATE END DATE * >%sysfunc(strip(&dp2)) " (cata2))
         year, (month, n);


define campaign / group '   ' width=10 style=[vjust=MIDDLE];
define camp_cate/ group noprint  width=12 format=$12.;
define camp_cate1/computed '  ' width=12 style=[vjust=MIDDLE] format=$12.;
define description/ group ' ' width=48 style=[vjust=MIDDLE] flow;
/*define c/sum'  '  STYLE(column)={TAGATTR='format:#,##'} width=15 flow;*/
define cata / sum "     "   STYLE(column)={TAGATTR='format:#,##'} width=20 flow;
define cata2 / sum "     " STYLE(column)={TAGATTR='format:#,##'} width=18 flow;  
define year / across '  '; 
define month / across ' ' width=3 format=cb. order=internal ;
define n/' ' STYLE(column)={TAGATTR='format:#,##'} width=4;


compute camp_cate1/char;
if camp_cate ne ' ' then hold=camp_cate;
camp_cate1=hold;
endcomp;


run;


*****D061*****;

ods tagsets.excelxp options(sheet_name='TSU by Customer - TV');		
title1 "MONTHLY ACTIVE CAMPAIGN -- SNAPSHOT DATE @%sysfunc(strip(&dp2))";
options missing='0';
ods escapechar='*';

proc report data=dddtve nowindows nocompletecols style(header)=[vjust=MIDDLE] missing;
column    camp_cate  
         ("CATEGORY" (camp_cate1))
         ("CAMPAIGN" (campaign)) 
         
         ("DESCRIPTION" (description))
         ("CAMPAIGN START DATE * USING MIN START DATE* (>%sysfunc(strip(&_pd1)), <%sysfunc(strip(&_pd2)))" (cata))
		 ("CAMPAIGN RATE END DATE * >%sysfunc(strip(&dp2)) " (cata2))
         year, (month, n);


define campaign / group '   ' width=10 style=[vjust=MIDDLE];
define camp_cate/ group noprint  width=12 format=$12.;
define camp_cate1/computed '  ' width=12 style=[vjust=MIDDLE] format=$12.;
define description/ group ' ' width=48 style=[vjust=MIDDLE] flow;
define cata / sum "     "   STYLE(column)={TAGATTR='format:#,##'} width=20 flow;
define cata2 / sum "     " STYLE(column)={TAGATTR='format:#,##'} width=18 flow;  
define year / across '  '; 
define month / across ' ' width=3 format=cb. order=internal ;
define n/' ' STYLE(column)={TAGATTR='format:#,##'} width=4;


compute camp_cate1/char;
if camp_cate ne ' ' then hold=camp_cate;
camp_cate1=hold;
endcomp;


run;

*****D062*****;

ods tagsets.excelxp options(sheet_name='TSU by Customer - INT');		
title1 "MONTHLY ACTIVE CAMPAIGN -- SNAPSHOT DATE @%sysfunc(strip(&dp2))";
options missing='0';
ods escapechar='*';

proc report data=dddint nowindows nocompletecols style(header)=[vjust=MIDDLE] missing;
column    camp_cate  
         ("CATEGORY" (camp_cate1))
         ("CAMPAIGN" (campaign)) 
         
         ("DESCRIPTION" (description))
         ("CAMPAIGN START DATE * USING MIN START DATE* (>%sysfunc(strip(&_pd1)), <%sysfunc(strip(&_pd2)))" (cata))
		 ("CAMPAIGN RATE END DATE * >%sysfunc(strip(&dp2)) " (cata2))
         year, (month, n);


define campaign / group '   ' width=10 style=[vjust=MIDDLE];
define camp_cate/ group noprint  width=12 format=$12.;
define camp_cate1/computed '  ' width=12 style=[vjust=MIDDLE] format=$12.;
define description/ group ' ' width=48 style=[vjust=MIDDLE] flow;
define cata / sum "     "   STYLE(column)={TAGATTR='format:#,##'} width=20 flow;
define cata2 / sum "     " STYLE(column)={TAGATTR='format:#,##'} width=18 flow;  
define year / across '  '; 
define month / across ' ' width=3 format=cb. order=internal ;
define n/' ' STYLE(column)={TAGATTR='format:#,##'} width=4;


compute camp_cate1/char;
if camp_cate ne ' ' then hold=camp_cate;
camp_cate1=hold;
endcomp;


run;

*****D063*****;

ods tagsets.excelxp options(sheet_name='TSU by Customer - RHP');		
title1 "MONTHLY ACTIVE CAMPAIGN -- SNAPSHOT DATE @%sysfunc(strip(&dp2))";
options missing='0';
ods escapechar='*';

proc report data=dddrhp nowindows nocompletecols style(header)=[vjust=MIDDLE] missing;
column    camp_cate  
         ("CATEGORY" (camp_cate1))
         ("CAMPAIGN" (campaign)) 
         
         ("DESCRIPTION" (description))
         ("CAMPAIGN START DATE * USING MIN START DATE* (>%sysfunc(strip(&_pd1)), <%sysfunc(strip(&_pd2)))" (cata))
		 ("CAMPAIGN RATE END DATE * >%sysfunc(strip(&dp2)) " (cata2))
         year, (month, n);


define campaign / group '   ' width=10 style=[vjust=MIDDLE];
define camp_cate/ group noprint  width=12 format=$12.;
define camp_cate1/computed '  ' width=12 style=[vjust=MIDDLE] format=$12.;
define description/ group ' ' width=48 style=[vjust=MIDDLE] flow;
define cata / sum "     "   STYLE(column)={TAGATTR='format:#,##'} width=20 flow;
define cata2 / sum "     " STYLE(column)={TAGATTR='format:#,##'} width=18 flow;  
define year / across '  '; 
define month / across ' ' width=3 format=cb. order=internal ;
define n/' ' STYLE(column)={TAGATTR='format:#,##'} width=4;


compute camp_cate1/char;
if camp_cate ne ' ' then hold=camp_cate;
camp_cate1=hold;
endcomp;


run;

*****D064*****;

ods tagsets.excelxp options(sheet_name='TSU by Customer - SHM');		
title1 "MONTHLY ACTIVE CAMPAIGN -- SNAPSHOT DATE @%sysfunc(strip(&dp2))";
options missing='0';
ods escapechar='*';

proc report data=dddshm nowindows nocompletecols style(header)=[vjust=MIDDLE] missing;
column    camp_cate  
         ("CATEGORY" (camp_cate1))
         ("CAMPAIGN" (campaign)) 
         
         ("DESCRIPTION" (description))
         ("CAMPAIGN START DATE * USING MIN START DATE* (>%sysfunc(strip(&_pd1)), <%sysfunc(strip(&_pd2)))" (cata))
		 ("CAMPAIGN RATE END DATE * >%sysfunc(strip(&dp2)) " (cata2))
         year, (month, n);


define campaign / group '   ' width=10 style=[vjust=MIDDLE];
define camp_cate/ group noprint  width=12 format=$12.;
define camp_cate1/computed '  ' width=12 style=[vjust=MIDDLE] format=$12.;
define description/ group ' ' width=48 style=[vjust=MIDDLE] flow;
define cata / sum "     "   STYLE(column)={TAGATTR='format:#,##'} width=20 flow;
define cata2 / sum "     " STYLE(column)={TAGATTR='format:#,##'} width=18 flow;  
define year / across '  '; 
define month / across ' ' width=3 format=cb. order=internal ;
define n/' ' STYLE(column)={TAGATTR='format:#,##'} width=4;


compute camp_cate1/char;
if camp_cate ne ' ' then hold=camp_cate;
camp_cate1=hold;
endcomp;


run;



ods tagsets.excelxp options (Absolute_Column_Width=' '
                             orientation='landscape'
                             embedded_titles='yes'
                             center_vertical='yes'
                             autofilter='1-2'
							 row_repeat='1-7'
		                     sheet_name='By Product'
	                         frozen_headers='yes'
							 autofit_height='yes'

     );
title1 "MONTHLY ACTIVE CAMPAIGN -- SNAPSHOT DATE @%sysfunc(strip(&dp2))"; 
options missing='0';
ods escapechar='*';
proc report data=d09 nowd split='*'  nocompletecols style(header)=[vjust=MIDDLE] missing;
column product ("PRODUCT" (prod1))
               ("CAMPAIGN" (campaign))
               ("CAMPAIGN START DATE * USING MIN START DATE* (>%sysfunc(strip(&_pd1)), <%sysfunc(strip(&_pd2)))" (cata))
		       ("CAMPAIGN RATE END DATE * >%sysfunc(strip(&dp2)) " (cata2))
                year, (month, n);
define product / group noprint; 
define prod1 / computed width=8  '   ' left style(header)=[vjust=MIDDLE];
define campaign / group '   ' width=10 ;
define cata / sum "     "   STYLE(column)={TAGATTR='format:#,##'} width=18 flow;
define cata2 / sum "     " STYLE(column)={TAGATTR='format:#,##'} width=24 flow;  
define year / across '  '; 
define month / across ' ' width=3 format=cb. order=internal ;
define n/' ' STYLE(column)={TAGATTR='format:#,##'} width=4;

compute prod1 / char;
if product ne ' ' then hold=product;
prod1=hold;
endcomp;

/*compute before _page_ /left;*/
/*/*line '*S={foot_weight=bold} this is a compute';*/*/
/*line "*S={font_weight=bold}WEEKLY ACTIVE CAMPAIGN -- SNAPSHOT DATE @%sysfunc(strip(&ss_date))"; */
/*line '   ';*/
/*line '   ';*/
/*endcomp;*/;*/;

run;
ods _all_ close;
ods listing;
title;
footnote;


/*define campaign / group '  ' width=10 style=[vjust=MIDDLE] style(column)=[cellwidth=.5in];*/



/**/
/*proc datasets lib=work nolist;*/
/*delete D:;*/
/*quit;*/
/*run;*/


