********************************************************************************
******** Do-file : Creation of the household configuration ******************* 
**** and partnership formation and separation in Belgian register data *********
********************************************************************************

** Version : 13/12/2021, work by Benjamin Marteau, Joan Damiens, Luisa Fadel,
** Océane Van Cleemput, Zuzana Zilincikova

* THIS DO FILE CONTAINS CODE:
* 1) Correcting of some the problems in the stock and flow files.........
* 2) Creation of the household configuration variable...................
* 3) Identifying partnership formation and separation variables.........

*Directories setup
global inpath 	`""/mnt/DEMO_FAMILYTIES/""'  		// directory of original data
global outpath 	`""/mnt/DEMO_SCHN/""'  	            	// directory of created data


// the directory of original data used in this do-file contains following datasets:
// BASIC FILE: t_2021_092_basis_c.dta
// STOCK FILES: t_2021_092_stock_1991_c.dta; t_2021_092_stock_1992_c.dta
// FLOW FILES: 	t_2021_092_tf_immigration_c.dta; t_2021_092_tf_emigration_c; t_2021_092_int_mig_inter_c; t_2021_092_int_mig_intra_c								

/*
This code will lead to the creation or correction of several .dta files : 

Household_var1992_FAMILYTIES & Household_var1993_FAMILYTIES : intermediate files with the new household composition variables (typmendet...) for 1992 and 1993

Flow1992, that gathers the migration information (intermunicipality, international emig and international immig; no intramunicipality info in 1992) for the year 1992
This files was built based on the 4 intermediate flow* files (flow_immigration.dta, flow_emigration.dta, flow_intra-m.dta and flow_internal.dta)

Deces1992, that gives info about deaths in 1992

Partnership_form : intermediate file with partnership formations in 1992, like the type of couple [MAR, UNM, LAT, NOC] (a few tiny corrections compared to the previous version)

Datepartnership_form: final file that gives information about household composition in 1992 and 1993 (including typmendet), about partnership formation (form_couple), its date (date_partner) and who moves (migform) in 1992; and partnership dissolution (end_couple), its date (datemove_partner) and who moves (migend) in 1992. 
*/

******************************************************************************** 
*********** Corrections of the date for specific years  ************************//TO CHECK IF NEEDED
********************************************************************************

*********** Stock files corrections for years 1991 to 2015
** Three modifications (year 1995, 1996, 2010) needed before running the code 
/*	

	**1995
	cd $inpath
	use 1995.dta, clear
	// Problem with nouvnumen that is different for the chief of household while 
	// the codernCM is the same for all household members
	bysort codernCM (n3lien) : replace nouvnumen=nouvnumen[_n+1] if ///
	codernCM==codernCM[_n+1] & n3lien==1 & ntaille>1 & com==com[_n+1]
	bysort codernCM (n3lien) : replace nouvnumen=nouvnumen[1] if ///
	codernCM==codernCM[1] & ntaille>1 & com==com[1]
	replace codernCM=codern if n3lien==1
	cd $outpath
	save 1995.dta, replace

	**1996
	cd $inpath
	use 1996.dta, clear
	// Problem with nouvnumen that is different for the chief of household while 
	//the codernCM is the same for all household members
	// ==> Replace the nouvnumen 
	bysort codernCM (n3lien) : replace nouvnumen=nouvnumen[_n+1] if ///
	codernCM==codernCM[_n+1] & n3lien==1 & ntaille>1 & com==com[_n+1] // 
	bysort codernCM (n3lien) : replace nouvnumen=nouvnumen[1] if ///
	codernCM==codernCM[1] & ntaille>1 & com==com[1]
	replace codernCM=codern if n3lien==1
	cd $outpath
	save 1996.dta, replace
	
	** 2010
	cd $inpath
	use 2010a.dta, clear
	// Replace codernPA = missing for codernPA where there is a blank
	replace codernPA="" if codernPA=="CB1443B1237D9150"
	replace codernPA="" if codernPA==" "
	cd $outpath
	save 2010.dta, replace
	
************ Flow files corrections
	cd $inpath
	use flux2006-2009.dta, clear
	rename TMIG tmig
	cd $outpath
	save flux2006-2009.dta, replace
	
	cd $inpath
	import sas using flux20102012.sas7bdat, clear
	// For flow files from 2010, there is no information on 
	// intramunicipality migration (tmig=28)									
	replace tmig="14" if tmig=="RI"
	replace tmig="24" if tmig=="RO"
	cd $outpath
	save flux20102012.dta, replace
	
	cd $inpath
	import sas using flux20132015.sas7bdat, clear
	replace tmig="14" if tmig=="RI"
	replace tmig="24" if tmig=="RO"
	cd $outpath
	save flux20132015.dta, replace

*/
	
cd $inpath
forvalues x=1992/2000{
use t_2021_092_stock_`x'_c.dta, clear
		bysort ID_DEMO_HH_HD_C (CD_REL_HH_HD) : replace ID_HH_C=ID_HH_C[1] if ///
	ID_DEMO_HH_HD_C==ID_DEMO_HH_HD_C[1] & hh_size>1 & CD_REFNIS==CD_REFNIS[1]
	
}


	

********************************************************************************
*********** Correcting information in the original stock variables**************
********************************************************************************

clear all
cd $inpath
use "t_2021_092_stock_1992_c.dta", clear								 		// adjust to the selected year
merge 1:1 ID_DEMO_C using t_2021_092_basis_c.dta
drop if _merge==2
drop _merge

****** Remove all the variables suffixes and I create a variable "year"
****** to easily reproduce the code for other years of the stock file

gen year=1992															// adjust to the selected year

/* ORIG VAR:
order year nouvnumen codern sex anaiss mnaiss jnaiss n3lien n3tymen neciv /// 
acheciv mcheciv jcheciv codernCM codernPA nat ntaille com
*/
order year ID_HH_C ID_DEMO_C cd_sex dt_bth CD_REL_HH_HD HH_TYPE_LIPRO CD_CIV ///
ID_DEMO_HH_HD_C ID_DEMO_PTNR_C CD_NATLTY hh_size CD_REFNIS

sort ID_HH_C CD_REL_HH_HD

** create label for variable CD_REL_HH_HD											
** no values 15-17 & 21-23 in 1991, variable still needs check

numlabel, add
tab CD_REL_HH_HD, miss
/*
1992
   Relation |
       with |
  Household |
  Head Code |      Freq.     Percent        Cum.
------------+-----------------------------------
         01 |  4,031,721       40.23       40.23
         02 |  2,338,706       23.34       63.56
         03 |  3,086,991       30.80       94.37
         04 |     23,488        0.23       94.60
         05 |     54,082        0.54       95.14
         06 |     31,905        0.32       95.46
         07 |     30,780        0.31       95.77
         08 |      1,135        0.01       95.78
         09 |     32,830        0.33       96.10
         10 |     10,834        0.11       96.21
         11 |     16,620        0.17       96.38
         12 |    211,733        2.11       98.49
         13 |     55,634        0.56       99.05
         14 |        771        0.01       99.05
         20 |     94,767        0.95      100.00
------------+-----------------------------------
      Total | 10,021,997      100.00

*/

destring CD_REL_HH_HD, replace
lab def CD_REL_HH_HD 1 "REFERENCE" 2 "husband,wife" 3 "son,daughter" ///
4 "child-in-law" 5 "granddaughter/grandson" 6 "father,mother" ///
7 "father-in-law,mother-in-law" 8 "grandfather,grandmother" ///
9 "brother, sister" 10 "brother-in-law,sister-in-law" 11 "other,related" ///
12 "other,	unrelated" 13 "step-children" ///
14 "great-granddaughter/grandson" 15"uncle,aunt" 16"nephew,niece" 17"cousin" ///
20 "community,home" 21"partner" 22"legal cohabitant" 23"comaternity"
lab value CD_REL_HH_HD CD_REL_HH_HD

gen acheciv= year(DT_STRT_CIV_STS_RRN)
gen mcheciv= month(DT_STRT_CIV_STS_RRN)
gen jcheciv= day(DT_STRT_CIV_STS_RRN)

gen anaiss= year(dt_bth)
gen mnaiss= month(dt_bth)
gen jnaiss= day(dt_bth)


** Correcting mistakes that are identified in the data before constructing 		
** the household variable defining household configuration variable
** NOTE : bysort year is used to account for observations from several years

* 1) Remove the ID_DEMO_PTNR_C (reffering to ID of spouse) when people are divorced 
*    or widowed
	** NOTE: we assume that information on marital statis is correct
	** 347,562 changesin 1992
	replace ID_DEMO_PTNR_C="" if inlist(CD_CIV,4,3)
	
* 2) Some ID_DEMO_PTNR_C are attributed to more than one person, we delete the info 	// for 1992 there are 192 cases
	bysort ID_DEMO_PTNR_C: gen PApb=1 if ID_DEMO_PTNR_C==ID_DEMO_PTNR_C[_n-1] ///
	& ID_DEMO_PTNR_C!=""
	bysort ID_DEMO_PTNR_C: replace PApb=1 if ///
	ID_DEMO_PTNR_C==ID_DEMO_PTNR_C[_n+1] & ID_DEMO_PTNR_C!=""
	replace ID_DEMO_PTNR_C="" if PApb==1
	drop PApb

* 3) Correcting the CD_REL_HH_HD between child and parent (when parent(s) are moving
*    to live with their child)
	bysort year ID_HH_C (CD_REL_HH_HD MS_AGE) : replace CD_REL_HH_HD=6 if  ///	//only 1 case in 1992
	CD_REL_HH_HD==3 & hh_size==2 & CD_REL_HH_HD[_n-1]==1 & MS_AGE>MS_AGE[_n-1]

* 4) Impute missing values for people whose civil status change at birth year
	replace acheciv=. if acheciv==anaiss & acheciv!=. 							//only 6 cases in 1992, only one same as dob, correct?
														// still strange

* 5) CD_REL_HH_HD: Distinguish amonng unrelated younger than 15 and those 16+ 
*    i.e. potential unmarried partners. 										
//need to create new category (24) or alternatively we could use age specific.
	replace CD_REL_HH_HD=24 if MS_AGE>15 & CD_REL_HH_HD==12	
	
		** Correction of the CD_REL_HH_HD label
		lab def CD_REL_HH_HD 1 "REFERENCE" 2 "husband,wife" 3 "son,daughter" ///
		4 "child-in-law" 5 "granddaughter/grandson" 6 "father,mother" ///
		7 "father-in-law,mother-in-law" 8 "grandfather,grandmother" ///
		9 "brother, sister" 10 "brother-in-law,sister-in-law" 11 "other,related" ///
		12 "other,unrelated, under16" 13 "step-children" ///
		14 "great-granddaughter/grandson" 15"uncle,aunt" 16"nephew,niece" 17"cousin" ///
		20 "community,home" 21"partner" 22"legal cohabitant" 23"comaternity" ///
		24 "other,unrelated, 16+", replace
		lab value CD_REL_HH_HD CD_REL_HH_HD

* 6) Correcting the CD_REL_HH_HD when people are married (CD_CIV=2), have the same
*    date of marriage but appear as unrelated in the data 
*    (or sometimes appear as children, son-in-law)
** 1,199 changes in 1992
	bysort year ID_HH_C (CD_REL_HH_HD) : replace CD_REL_HH_HD=2 if ID_DEMO_PTNR_C==ID_DEMO_C[1] ///
	& CD_CIV==2 & DT_STRT_CIV_STS_RRN==DT_STRT_CIV_STS_RRN[1]

* 7) Correcting the ID_DEMO_PTNR_C when people are married (CD_CIV=2), have the same
*    date of marriage but are not linked with ID_DEMO_PTNR_C
** 3,010 + 1,885 changes in 1992
	bysort year ID_HH_C (CD_REL_HH_HD ID_DEMO_C) : replace ID_DEMO_PTNR_C= ///
	ID_DEMO_C[_n+1] if DT_STRT_CIV_STS_RRN==DT_STRT_CIV_STS_RRN[_n+1] ///
	& CD_CIV==CD_CIV[_n+1] & CD_CIV==2

	bysort year ID_HH_C (CD_REL_HH_HD ID_DEMO_C) : replace ID_DEMO_PTNR_C=ID_DEMO_C[_n-1] ///
	if DT_STRT_CIV_STS_RRN==DT_STRT_CIV_STS_RRN[_n-1] & CD_CIV==CD_CIV[_n-1] & CD_CIV==2

* 8) Correcting the CD_REL_HH_HD when a person is identified as a spouse (CD_REL_HH_HD=2) 
*    although the couple is not married (CD_CIV!=2)
** 2,410 + 1,769 changes
	bysort year ID_HH_C (CD_REL_HH_HD): replace CD_REL_HH_HD=24 if ID_DEMO_PTNR_C=="" ///
	& CD_REL_HH_HD==2 & inlist(CD_CIV,1,3,4) & CD_REL_HH_HD[1]==1 & MS_AGE>15
	bysort year ID_HH_C (CD_REL_HH_HD): replace CD_REL_HH_HD=12 if ID_DEMO_PTNR_C=="" ///
	& CD_REL_HH_HD==2 & inlist(CD_CIV,1,3,4) & CD_REL_HH_HD[1]==1 & MS_AGE<=15
	
* 9)  Problem with hh identifier that is different for the chief of household while 
	// the hhh identifier is the same for all household members
	bysort ID_DEMO_HH_HD_C (CD_REL_HH_HD) : replace ID_HH_C=ID_HH_C[1] if ///
	ID_DEMO_HH_HD_C==ID_DEMO_HH_HD_C[1] & hh_size>1 & CD_REFNIS==CD_REFNIS[1]
	
** No need for all years
/* Changes made = 
	- 1992 -> 13,245 changes
	- 1993 -> 12,734 changes
	- 1994 -> 12,618 changes
	- 1995 -> 13,630 changes
	- 1996 -> 10,782 changes
	- 1997 -> 9,067 changes
	- 1998 -> 8,497 changes
	- 1999 -> 7,947 changes
	- 2000 -> 7,002 changes
	
	- 2008 -> 861 changes
	- 2009 -> 2 changes

	- 2012 -> 278 changes
	- 2013 -> 112 changes
	- 2014 -> 54 changes
	- 2015 -> 50 changes
	- 2016 -> 7 changes
	- 2017 -> 25 changes
	- 2018 -> 3 changes
*/

	
********************************************************************************
******** Creating indicators used for constructing of a new household **********
********************************************************************************

** Number of unrelated individuals aged 16+ by household
	egen ncoh=total(CD_REL_HH_HD==24), by(year ID_HH_C)
	recode ncoh (0=0) (1=0) (2/999=1), gen(ncohlab)
** Number of related individuals by household
	egen napp=total(inlist(CD_REL_HH_HD,2,3,4,5,6,7,8,9,10,11,13,14,15,16,17)), ///
	by(year ID_HH_C)
** Number of related individuals (except spouse and children of head of household)
	egen nappnonenf=total(inlist(CD_REL_HH_HD,4,5,6,7,8,9,10,11,14,15,16,17)), ///
	by(year ID_HH_C)
** Number of other unrelated aged -15
	egen nautapp=total(CD_REL_HH_HD==12), by(year ID_HH_C)
** Number of children of head of household
	egen nenf=total(CD_REL_HH_HD==3), by(year ID_HH_C)
** Number of children aged 16+ by household
	egen nenf16=total(CD_REL_HH_HD==3 & MS_AGE>=16), by(year ID_HH_C)
** Number of stepchildren
	egen nbelenf=total(CD_REL_HH_HD==13), by(year ID_HH_C)
** Number of stepchildren aged 16 and more
	egen nbelenf16=total(CD_REL_HH_HD==13 & MS_AGE>=16), by(year ID_HH_C)
** Number of parents or parents-in-law in the household
	egen nparents=total(inlist(CD_REL_HH_HD,6,7)), by(year ID_HH_C)
** Number of children-in-law
	egen ngendre=total(CD_REL_HH_HD==4), by(year ID_HH_C)
** Number of grandchildren of head of household
	egen npetits=total(CD_REL_HH_HD==5), by(year ID_HH_C)
** Number of grandparents of head of household
	egen ngrands=total(CD_REL_HH_HD==8), by(year ID_HH_C)
** Number of spouses in the household
	egen nepoux=total(CD_REL_HH_HD==2), by (year ID_HH_C)
** Number of brothers and sisters in the household
	egen nfreres=total(CD_REL_HH_HD==9), by (year ID_HH_C)
** Mean age by household
	egen meanage=mean(MS_AGE), by(year ID_HH_C)
	capture drop meanagegroup
	recode meanage (0/15=0 "<=15") (15.00001/19.999999=1 "15/19") ///
					(20/24.999999=2 "20/24") (25/29.999999=3 "25/29") ///
					(30/34.999999=4 "30/34") (35/39.999999=5 "35/39") ///
					(40/44.999999=6 "40/44") (45/49.999999=7 "45/49") ///
					(50/54.999999=8 "50/54") (55/59.999999=9 "55/59") ///
					(60/64.999999=10 "60/64") (65/69.999999=11 "65/69") ///
					(70/74.999999=12 "70/74") (75/79.999999=13 "75/79") /// 
					(80/120=14 "80+") , gen(meanagegroup)

** Number of individuals where the age difference between individual and the 
** mean is between -8 and +8: created to identify flatsharing
	capture drop ndiffage8
	egen ndiffage8=total(inrange(meanage-MS_AGE,-8,8)), by(year ID_HH_C)
	drop meanage

** Presence of a married partner in the household 
**(when codernPA does refer to a codern in the household)
capture drop conjmar
bysort year ID_HH_C (CD_REL_HH_HD): gen conjmar=1 if ID_DEMO_C==ID_DEMO_PTNR_C[_n+1] & ///
ID_DEMO_PTNR_C==ID_DEMO_C[_n+1] & CD_CIV==2 & CD_CIV[_n+1]==2 & HH_TYPE_LIPRO!=8

forvalues x=1/15 {
bysort year ID_HH_C (CD_REL_HH_HD): replace conjmar=1 if ID_DEMO_C==ID_DEMO_PTNR_C[_n+`x'] ///
& ID_DEMO_PTNR_C==ID_DEMO_C[_n+`x'] & CD_CIV==2 & CD_CIV[_n+`x']==2 ///
& HH_TYPE_LIPRO!=8 & conjmar==.
}

forvalues x=1/15 {
bysort year ID_HH_C (CD_REL_HH_HD): replace conjmar=1 if ID_DEMO_C==ID_DEMO_PTNR_C[_n-`x'] ///
& ID_DEMO_PTNR_C==ID_DEMO_C[_n-`x'] & CD_CIV==2 & CD_CIV[_n-`x']==2 ///
& HH_TYPE_LIPRO!=8 & conjmar==.
}

replace conjmar=0 if conjmar==.
compress

********************************************************************************
** Creation of the household configuration variable ****************************
********************************************************************************
** Now we have identified and corrected some main problems in the data.
** We can start with the construction of the type of household variable.
** This is a detailed variable (named typmenndet) based on the information 
** on the household members.
** We distinguish in the household categories between "standard" households where
** only close family members make up the household, and "complex" households with 
** a family nucleus + distant family (cousin, uncle...) or non-family members 
** who are identified as unrelated.
** Remember this is the residential situation, couples without children 
** in the household do not mean that they are childless.

* NOTE: If there are more than 1 household member, two lines of code are required. 
*       First line refers to head of the household, second line to other hh members

capture drop typmendet

************** IH : Solo living Men *******************************************
gen typmendet="IH" if cd_sex==1 & hh_size==1 & CD_REL_HH_HD==1 & MS_AGE>15

// Remaining problems : People living solo aged less than 16 : parents that are 
// asylum seekers or foreigners appear on a specific register and are not 
// recorded in the standard register. We decide to restrict 
// a person living solo to people aged 16+, and to categorize children 
// living virtually solo as "other"

************** IF : Solo living Women ******************************************
replace typmendet="IF" if cd_sex==2 & hh_size==1 & CD_REL_HH_HD==1 & MS_AGE>15

************** MH : Single-parent family with a man HH *************************

***** Standard household (MHs)
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="MHs" if CD_REL_HH_HD==1 & cd_sex==1 ///
& CD_REL_HH_HD[_n+1]==3 & nenf==(hh_size-1) 

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="MHs" if CD_REL_HH_HD!=1 ///
 & typmendet[_n-1]=="MHs"

***** Complex household (MHi)
** Only for households without parents or without grandchildren that are  
** identified as multigenerational households later 

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="MHi" if CD_REL_HH_HD==1 ///
& cd_sex==1 & CD_REL_HH_HD[_n+1]==3 & nenf<(hh_size-1) & ncoh==0 & ngendre==0 ///
& nparents==0 & npetits==0

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="MHi" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="MHi"

************** MF : Single-parent family with a woman HH ***********************
***** Standard household (MFs)
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="MFs" if CD_REL_HH_HD==1 & cd_sex==2 ///
& CD_REL_HH_HD[_n+1]==3 & nenf==(hh_size-1)

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="MFs" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="MFs"

***** Complex household (MFi)
** Only for households without parents or without grandchildren that are 
** identified as multigenerational households later 

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="MFi" if CD_REL_HH_HD==1 ///
& cd_sex==2 & CD_REL_HH_HD[_n+1]==3 & nenf<(hh_size-1) & ncoh==0 & ngendre==0 ///
& nparents==0 & npetits==0

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="MFi" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="MFi" 

*************** CSEs : Standard Childless married couples *********************
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="CSEs" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+1]==2 & hh_size==2 & CD_CIV==2 & DT_STRT_CIV_STS_RRN==DT_STRT_CIV_STS_RRN[_n+1] ///
& ID_DEMO_C[_n+1]==ID_DEMO_PTNR_C

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="CSEs" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="CSEs"

*************** CSEi : Complex Childless married couples *********************
** Only for household without (grand)parents that are identified as 
** multigenerational households later 
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="CSEi" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+1]==2 & hh_size>2 & CD_CIV==2 & DT_STRT_CIV_STS_RRN==DT_STRT_CIV_STS_RRN[_n+1] ///
& nenf==0 & nbelenf==0 & ngrands==0 & nparents==0 ///
& ID_DEMO_C[_n+1]==ID_DEMO_PTNR_C

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="CSEi" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="CSEi"

*************** CAEs : Standard Married couples with children ******************

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="CAEs" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+1]==2 & hh_size>2 & CD_CIV==2 & DT_STRT_CIV_STS_RRN==DT_STRT_CIV_STS_RRN[_n+1] ///
& ID_DEMO_PTNR_C==ID_DEMO_C[_n+1] & (nenf+nbelenf)==(hh_size-2)

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="CAEs" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="CAEs"

*************** CAEi : Complex Married couples with children *******************
** Complex married couples with children with related or >16 unrelated individuals 

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="CAEi" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+1]==2 & hh_size>3 & CD_CIV==2 & DT_STRT_CIV_STS_RRN==DT_STRT_CIV_STS_RRN[_n+1] ///
& ID_DEMO_PTNR_C==ID_DEMO_C[_n+1] & (nbelenf>0|nenf>0) ///
& (nenf+nbelenf)<(hh_size-2) & ngendre==0 & ncoh==0 & nparents==0 ///
& ngrands==0 & npetits==0

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="CAEi" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="CAEi"

**************** CoSs : Standard Childless Different-sex Unmarried couple ******
** For unmarried couples, we do not distinguish complex household situations 
** as we lack information on the relations between individuals 
** We adopt a broad definition of unmarried couples: two unrelated individuals  
** living in the same household, regardless of the age difference           

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="CoSs" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+1]==24 & cd_sex!=cd_sex[_n+1] & hh_size==2

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="CoSs" if CD_REL_HH_HD!=1 ///
 & typmendet[_n-1]=="CoSs"

**************** CoAs : Standard Unmarried Different-sex Couples with Children *
*Note: Limited to household with exactly one unrelated individual
forvalues x=2/16 {
bysort year ID_HH_C (CD_REL_HH_HD MS_AGE) : replace typmendet="CoAs" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+`x']==24 & cd_sex!=cd_sex[_n+`x'] & hh_size>2 ///
& (nenf+nbelenf)==(hh_size-2) & (nbelenf>0|nenf>0) & ncoh<=1 & nappnonenf==0 ///
& nepoux==0 & conjmar==0 & conjmar[_n+`x']==0
} // For children of head of household

forvalues x=2/16 {
bysort year ID_HH_C (CD_REL_HH_HD MS_AGE) : replace typmendet="CoAs" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+`x']==24 & cd_sex!=cd_sex[_n+`x'] & hh_size>2 ///
& (nenf+nautapp)==(hh_size-2) & (nautapp>0|nenf>0) & ncoh<=1 & nappnonenf==0 ///
& nepoux==0 & conjmar==0 & conjmar[_n+`x']==0
} // For children of unmarried partner

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="CoAs" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="CoAs"



*************** HoSs : Standard Childless Same-sex Unmarried couple (?) ********
** This category is not reliable, many couples in this category are probably not 
** same-sex couples (if we follow the individuals over time, they are likely to
** forming a different-sex union later in the life course)
** We nevertheless, create the category 

// Two same-sex individuals without other household members
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="HoSs" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+1]==24 & cd_sex==cd_sex[_n+1] & hh_size==2  

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="HoSs" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="HoSs"

*************** HoAs : Standard Unmarried Same-sex couples with children (?) ***
forvalues x=2/16 {
bysort year ID_HH_C (CD_REL_HH_HD MS_AGE) : replace typmendet="HoAs" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+`x']==24 & cd_sex==cd_sex[_n+`x'] & hh_size>2 /// 
& (nenf+nbelenf)==(hh_size-2) & (nbelenf>0|nenf>0) & ncoh<=1 & nappnonenf==0 ///
& nepoux==0 & conjmar==0 & conjmar[_n+`x']==0
} // For children of head of household

forvalues x=2/16 {
bysort year ID_HH_C (CD_REL_HH_HD MS_AGE) : replace typmendet="HoAs" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+`x']==24 & cd_sex==cd_sex[_n+`x'] ///
& hh_size>2 &(nenf+nautapp)==(hh_size-2) & (nautapp>0|nenf>0) & ncoh<=1 ///
 & nappnonenf==0 & nepoux==0 & conjmar==0 & conjmar[_n+`x']==0
} // For children of unmarried partner

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="HoAs" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="HoAs"

******************** Multi : Multigenerational households **********************

**** Multi_MA : Households with 3 generations (Grandparent + Parent + Child/ren) 
** where the head of the household is married whether this is the Parent 
** or the Grand-Parent

** Head is the parent (middle-generation)
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Multi_MA" ///
if CD_REL_HH_HD==1 & CD_REL_HH_HD[_n+1]==2 & hh_size>3 & CD_CIV==2 ///
& DT_STRT_CIV_STS_RRN==DT_STRT_CIV_STS_RRN[_n+1] & ID_DEMO_PTNR_C==ID_DEMO_C[_n+1] ///
& nparents>0 & (nenf>0|nbelenf>0) & ncoh==0

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Multi_MA" ///
if CD_REL_HH_HD!=1 & typmendet[_n-1]=="Multi_MA"

** Head if the Grandparent (older generation)
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Multi_MA" ///
if CD_REL_HH_HD==1 & CD_REL_HH_HD[_n+1]==2 & hh_size>3 & CD_CIV==2 ///
& DT_STRT_CIV_STS_RRN==DT_STRT_CIV_STS_RRN[_n+1] & ID_DEMO_PTNR_C==ID_DEMO_C[_n+1] ///
& (nenf>0|nbelenf>0) & npetits>0 & ncoh==0

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Multi_MA" ///
if CD_REL_HH_HD!=1 & typmendet[_n-1]=="Multi_MA"

******** Multi_Co : Households with 3 generations (GP + Parent + Child/ren) **** 
** where the head of the household is cohabitating (unmarried) with a partner 
** whether this is the Parent or the Grand-Parent

** Head is the parent (middle-generation)
forvalues x=1/15 {
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Multi_Co" if CD_REL_HH_HD==1 ///
& nepoux==0 & nparents>0 & ncoh==1 & ID_DEMO_PTNR_C=="" & CD_REL_HH_HD[_n+`x']==24 ///
& ID_DEMO_PTNR_C[_n+`x']=="" & (nenf>0|nbelenf>0|nautapp>0)
}

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Multi_Co" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="Multi_Co"

** Head is the Grandparent (older-generation)
forvalues x=1/15 {
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Multi_Co" if CD_REL_HH_HD==1 ///
& nepoux==0 & ncoh==1 & ID_DEMO_PTNR_C=="" & CD_REL_HH_HD[_n+`x']==24 ///
& ID_DEMO_PTNR_C[_n+`x']=="" & (nenf>0|nbelenf>0) & npetits>0
}

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Multi_Co" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="Multi_Co"

********* Multi_Solo : Households with 3 generations (GP + Parent + Child/ren) 
** where the head of the household is single whether the Parent or the Grand-Parent

** Head is the parent (middle-generation)
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Multi_Solo" if CD_REL_HH_HD==1 ///
& ncoh==0 & nepoux==0 & hh_size>2 & nparents>0 & (nenf>0|nbelenf>0|nautapp>0)

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Multi_Solo" ///
if CD_REL_HH_HD!=1 & typmendet[_n-1]=="Multi_Solo"

** Head is the Grandparent (older-generation)
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Multi_Solo" if CD_REL_HH_HD==1 ///
& ncoh==0 & nepoux==0 & hh_size>2 & (nenf>0|nbelenf>0) & npetits>0

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Multi_Solo" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="Multi_Solo"

**** O_MA : Household with a married couple and at least one older parent(in-law) 
** Head is the parent (middle-generation)
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="O_MA" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+1]==2 & hh_size>=3 & CD_CIV==2 & DT_STRT_CIV_STS_RRN==DT_STRT_CIV_STS_RRN[_n+1] ///
& ID_DEMO_PTNR_C==ID_DEMO_C[_n+1] & nparents>0 ///
& nenf==0 & nbelenf==0 & nautapp==0 & ncoh==0

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="O_MA" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="O_MA"

** N.B. If Head is the "GP" in a married couple (older generation) => CAE
** Problem : Pluricouple ?

******** O_Co : Household with a cohabitant couple and at least an older parent
**Head is the parent (middle-generation)
forvalues x=1/10 {
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="O_Co" if CD_REL_HH_HD==1 ///
& nepoux==0 & nparents>0 & ncoh==1 & ID_DEMO_PTNR_C=="" & CD_REL_HH_HD[_n+`x']==24 ///
& ID_DEMO_PTNR_C[_n+`x']=="" & nenf==0 & nbelenf==0 & nautapp==0
}

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="O_Co" ///
if CD_REL_HH_HD!=1 & typmendet[_n-1]=="O_Co"

** N.B. If Head is the "GP" (older generation) => CoA

********* O_Solo : Household with a single individual and one or more parent
replace typmendet="O_Solo" if CD_REL_HH_HD==1 & ncoh==0 & nepoux==0 ///
& hh_size>=2 & nparents>0 & nenf==0 & nbelenf==0 & nautapp==0

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="O_Solo" ///
if CD_REL_HH_HD!=1 & typmendet[_n-1]=="O_Solo"

*********** Pluri : Households with at least two couples 
** (Parents + children and their married partners)
** Parents married, children married (head is the parent)
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Pluri" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+1]==2 & hh_size>3 & CD_CIV==2 & DT_STRT_CIV_STS_RRN==DT_STRT_CIV_STS_RRN[_n+1] ///
& ID_DEMO_PTNR_C==ID_DEMO_C[_n+1] ///
& (nenf>0|nbelenf>0) & ngendre>0 & nparents==0 & npetits==0 

** Parents unmarried, children married
forvalues x=1/10 {
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Pluri" if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_n+`x']==24 & hh_size>3 & ID_DEMO_PTNR_C=="" & ncoh==1 ///
& (nenf>0|nbelenf>0) & ngendre>0 & nparents==0 & npetits==0 
}

** NB : If head is the child and parents married or not ==> O_MA

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Pluri" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="Pluri"

************** FS : Brothers and sisters only **********************************
bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="FS" if CD_REL_HH_HD==1 ///
& hh_size==nfreres+1 & nfreres>0

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="FS" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="FS"

*************** Coloc : Flatsharing *********************
** We define flatsharing as more than 2 persons who are unrelated with regards 
** to the head of household, where there are only unrelated individuals in the  
** household and where the age difference is less then 8 years. 
** We limit the size of household to 16. 

replace typmendet="Coloc" if CD_REL_HH_HD==1 & hh_size>2 & ncoh>1 ///
& ncoh==(hh_size-1) & ndiffage8==hh_size & HH_TYPE_LIPRO!=8

bysort year ID_HH_C (CD_REL_HH_HD) : replace typmendet="Coloc" if CD_REL_HH_HD!=1 ///
& typmendet[_n-1]=="Coloc"

************** Coll : Collective households, institutions **********************
replace typmendet="Coll" if HH_TYPE_LIPRO==8 & typmendet=="" 

************** Aut : Other households ****************************
** All the configurations we were not able to identify

replace typmendet="Aut" if typmendet==""

/** typmendet variable categories:
IH "Solo living man" 
IF "Solo living woman"
MHs "Standard single-parent family with a man HH"
MHi "Complex single-parent family with a man HH" 
MFs "Standard single-parent family with a woman HH"
MFi "Complex single-parent family with a woman HH" 
CSEs "Standard childless married couples" 
CSEi "Complex childless married couples with related or young unrelated individuals"
CAEs "Standard married couples with children"
CAEi "Complex married couples with children with related or young unrelated individuals"
CoSs "Standard childless different-sex unmarried couple"
CoAs "Standard unmarried different-sex couples with children"
HoSs "Standard childless same-sex unmarried couples"
HoAs "Standard same-sex unmarried couples with children"
Multi_MA "3 generational household, head married"
Multi_Co "3 generational household, head cohabitating"
Multi_Solo "3 generational household, head single"
O_MA "2 generational households, married child is the head of the household"
O_Co "2 generational household, cohabiting child is the head of the household"
O_Solo "2 generational househols, single child is the head of the household"
FS "Brothers and sisters only"
Pluri "Two couples household, parent is the head of the household"
Coloc "Flatsharing"
Coll "Collective households and institutions"
Aut "Other households"   
*/


** Verifications and comparisons with older variable
tab typmendet HH_TYPE_LIPRO if CD_REL_HH_HD==1, m
tab typmendet HH_TYPE_LIPRO, m

********************************************************************************
** Creation of a cruder household composition variable  ************************
********************************************************************************
capture drop typmendet_short
************** IH : Solo Men **********************
gen typmendet_short="IH" if typmendet=="IH"

************** IF : Solo Women **********************
replace typmendet_short="IF" if typmendet=="IF"

************** MH : Single-parent Men *****************
replace typmendet_short="MH" if inlist(typmendet,"MHs","MHi")

************** MF : Single-parent Women ******************
replace typmendet_short="MF" if inlist(typmendet,"MFs","MFi")

************** CSE : Childless Married Couples **********************
replace typmendet_short="CSE" if inlist(typmendet,"CSEs","CSEi")

************* CAE : Married Couples with Children ********************
replace typmendet_short="CAE" if inlist(typmendet,"CAEs","CAEi")

************* CoS : Childless >< sex Unmarried Couples ********************
replace typmendet_short="CoS" if inlist(typmendet,"CoSs")

************* CoA : Unmarried >< sex Couples with Children ********************
replace typmendet_short="CoA" if inlist(typmendet,"CoAs")

************* HoS : Childless Unmarried Same-sex Couples *************
replace typmendet_short="HoS" if inlist(typmendet,"HoSs")

************* HoA : Unmarried Same-sex Couples with Children *************
replace typmendet_short="HoA" if inlist(typmendet,"HoAs")

************* Coll : Collective Household, Institutions ************************
replace typmendet_short="Coll" if typmendet=="Coll" 

************* Other ****************************************
replace typmendet_short="Aut" if ///
inlist(typmendet,"Aut","Coloc","Multi_MA","Multi_Co","Multi_Solo")

replace typmendet_short="Aut" if inlist(typmendet,"FS","Pluri","O_Co","O_MA","O_Solo")

/*variable typmendet_short 
IH "Solo living man" 
IF "Solo living woman"
MH "Single-parent family with a man HH"
MF "Single-parent family with a woman HH"
CSE "Childless married couples"
CAE "Married couples with children"
CoS "Childless different-sex unmarried couple"  
CoA "Unmarried different-sex couples with children" 
HoS "Childless same-sex unmarried couples"
HoA "Same-sex unmarried couples with children"
Coll "Collective households and institutions"
Aut "Other households"   
*/

tab typmendet typmendet_short
tab typmendet_short HH_TYPE_LIPRO, m

compress


***************************
/* Creation of a dummy variable that identifies when there are two married 
couples within a multigenerational household [in which the head is married */

capture drop Multi_MA_2
gen Multi_MA_2=0
replace Multi_MA_2=1 if typmendet=="Multi_MA" & inlist(CD_REL_HH_HD, 3, 6, 7) ///
& CD_CIV==2

forvalues x=1/15 {
bysort year ID_HH_C (CD_REL_HH_HD) : replace Multi_MA_2=1 if Multi_MA_2[_n+`x']==1
}

bysort year ID_HH_C (CD_REL_HH_HD) : replace Multi_MA_2=1 if Multi_MA_2[_n-1]==1

/* Same, but in a hh in which the head is assumed as cohabitant */
capture drop Multi_Co_MA
gen Multi_Co_MA=0
replace Multi_Co_MA=1 if typmendet=="Multi_Co" & inlist(CD_REL_HH_HD, 3,6,7) & CD_CIV==2

forvalues x=1/15 {
bysort year ID_HH_C (CD_REL_HH_HD) : replace Multi_Co_MA=1 if Multi_Co_MA[_n+`x']==1
}

bysort year ID_HH_C (CD_REL_HH_HD) : replace Multi_Co_MA=1 if Multi_Co_MA[_n-1]==1

** Save database
cd $outpath
save Household_var1993_FAMILYTIES.dta, replace									// adjust to the selected year


****************************************************************************
**************** Partnership Formation and Separation variables ************
****************************************************************************
cd $outpath
use Household_var1992_FAMILYTIES.dta, clear

** When you have saved the new file for 1993, you can append it to 1992
append using Household_var1993_FAMILYTIES.dta


** You can tabulate the typmendet to see how close they are btw two years
tab typmendet year, m

************** Creation of an unmarried partner code
/*Equivalent to code for the married partner (codernPA), we create 
a code for unmarried partner in the same household (codernPNM). 
PNM stands for "Partner not married" */
 
capture drop codernPNM
bysort year ID_HH_C (CD_REL_HH_HD) : gen codernPNM=ID_DEMO_C[_N] if CD_REL_HH_HD==1 ///
& CD_REL_HH_HD[_N]==24 & CD_REL_HH_HD[_n+1]!=2 ///
& inlist(typmendet,"CoAs","CoAi","CoSs","HoAs","HoSs","Multi_Co","O_Co","Pluri") 

bysort year ID_HH_C (CD_REL_HH_HD) : replace codernPNM=ID_DEMO_C[1] if CD_REL_HH_HD==24 ///
 & CD_REL_HH_HD[1]==1 & ID_DEMO_C==codernPNM[1] & ///
 inlist(typmendet,"CoAs","CoAi","CoSs","HoAs","HoSs","Multi_Co","O_Co","Pluri")

tab CD_REL_HH_HD if codernPNM!="", m
tab typmendet CD_CIV if codernPNM!="", m

***************** Creation of a code for the couple, married or not ************

/* NB : Some people have their married partners in the household (codernPA) 
but appear unmarried, because of an incorrect information of the civil status,
for example. The creation of the codecouple solves the problem */

capture drop codecouple

// For married couples
// The rule to create the code is to begin with the code of the partner who's 
// first in the alphabetical order (codern>codernPA)
bysort year ID_HH_C (CD_REL_HH_HD) : gen codecouple=ID_DEMO_C+ID_DEMO_PTNR_C if ///
ID_DEMO_C>ID_DEMO_PTNR_C & ID_DEMO_C==ID_DEMO_PTNR_C[_n+1] & ID_DEMO_PTNR_C==ID_DEMO_C[_n+1] & CD_CIV==2 ///
& CD_CIV[_n+1]==2

bysort year ID_HH_C (CD_REL_HH_HD) : replace codecouple=ID_DEMO_PTNR_C+ID_DEMO_C if ///
ID_DEMO_C<ID_DEMO_PTNR_C & ID_DEMO_C==ID_DEMO_PTNR_C[_n+1] & ID_DEMO_PTNR_C==ID_DEMO_C[_n+1] & CD_CIV==2 ///
& CD_CIV[_n+1]==2

forvalues x=2/30 {
bysort year ID_HH_C (CD_REL_HH_HD) : replace codecouple=ID_DEMO_C+ID_DEMO_PTNR_C if ///
ID_DEMO_C>ID_DEMO_PTNR_C & ID_DEMO_C==ID_DEMO_PTNR_C[_n+`x'] & ID_DEMO_PTNR_C==ID_DEMO_C[_n+`x'] & ///
CD_CIV==2 & CD_CIV[_n+`x']==2
}

forvalues x=2/30 {
bysort year ID_HH_C (CD_REL_HH_HD) : replace codecouple=ID_DEMO_PTNR_C+ID_DEMO_C if ///
ID_DEMO_C<ID_DEMO_PTNR_C & ID_DEMO_C==ID_DEMO_PTNR_C[_n+`x'] & ID_DEMO_PTNR_C==ID_DEMO_C[_n+`x'] ///
& CD_CIV==2 & CD_CIV[_n+`x']==2
}

forvalues x=1/30 {
bysort year ID_HH_C (CD_REL_HH_HD) : replace codecouple=ID_DEMO_C+ID_DEMO_PTNR_C if ///
ID_DEMO_C>ID_DEMO_PTNR_C & ID_DEMO_C[_n-`x']==ID_DEMO_PTNR_C & ID_DEMO_C==ID_DEMO_PTNR_C[_n-`x'] ///
 & CD_CIV==2 & CD_CIV[_n-`x']==2
}

forvalues x=1/30 {
bysort year ID_HH_C (CD_REL_HH_HD) : replace codecouple=ID_DEMO_PTNR_C+ID_DEMO_C if ///
ID_DEMO_C<ID_DEMO_PTNR_C & ID_DEMO_C[_n-`x']==ID_DEMO_PTNR_C & ID_DEMO_C==ID_DEMO_PTNR_C[_n-`x'] ///
 & CD_CIV==2 & CD_CIV[_n-`x']==2
}

// For unmarried couples
replace codecouple=ID_DEMO_C+codernPNM if ID_DEMO_C>codernPNM & codernPNM!="" & ///
inlist(typmendet,"CoAs","CoAi","CoSs","HoAs","HoSs","Multi_Co","O_Co","Pluri") ///
& codecouple==""
replace codecouple=codernPNM+ID_DEMO_C if ID_DEMO_C<codernPNM & codernPNM!="" & ///
inlist(typmendet,"CoAs","CoAi","CoSs","HoAs","HoSs","Multi_Co","O_Co","Pluri") ///
& codecouple==""

/*
// if you want to create a code for married couples without 
// any potential partner in the household
gen codecouple_abs=codern+codernPA if codern>codernPA & codernPA!="" & ///
neciv==20 & codecouple==""

replace codecouple_abs=codernPA+codern if codern<codernPA & codernPA!="" ///
& neciv==20 & codecouple==""
*/

** NB : If you want to study collective institutions, 
** you have to include a lot of loop to find back the partners

compress

** Creation of variables that use the code of the married or unmarried partner *
** Partner's birth year, sex, nationality, civil status
local list dt_bth cd_sex CD_NATLTY CD_CIV

foreach var of local list {
bysort year ID_HH_C (codecouple) : ///
gen `var'_partner=`var'[_n+1] if codecouple==codecouple[_n+1] & codecouple!=""
bysort year ID_HH_C (codecouple) : ///
replace `var'_partner=`var'[_n-1] if codecouple==codecouple[_n-1] & codecouple!=""
}

**************** Partnership situation variable

capture drop couple 
// married couple
gen couple=1 if codecouple!="" & ID_DEMO_PTNR_C!="" & CD_CIV==2 & codernPNM=="" ///
& dt_bth_partner!=.

// unmarried couple
replace couple=2 if codecouple!="" & codernPNM!="" & couple==.

// married without any partner in the household
replace couple=3 if codecouple=="" & ID_DEMO_PTNR_C!="" & CD_CIV==2 & couple==. ///
& dt_bth_partner==.

// no couple
replace couple=9 if couple==.

label define coupleb 1 "MAR" 2 "UNM" 3 "LAT" 9 "NOC" , modify
label value couple coupleb

tab MS_AGE couple, row nofreq m
tab year couple, row nofreq m

compress 

***************************************************************
******* Creation of a variable of union formation *************
***************************************************************
** If an individual has zero cohabiting (married or not) partner in the household
** in t-1 and a partner in t, is it coded as an union formation.

** If single or married LAT in t-1 (1991) and married or unmarried co-resident 
** couple in t (1992)
bysort ID_DEMO_C (year) : gen form_couple=1 if inlist(couple[_n-1],3,9) & ///
inlist(couple,1,2) 

** Partnered in t-1 and t but with a differnt partner
bysort ID_DEMO_C (year) : replace form_couple=2 if inlist(couple[_n-1],1,2) ///
& inlist(couple,1,2) & codecouple[_n-1]!=codecouple & codecouple!="" 

replace form_couple=0 if form_couple==. 
label def form_couple 1 "New, single" 2 "New, repartner"
label val form_couple form_couple 

*NB: The number of union formations among "same-sex couples" is overestimated
/* "Solution" : create a category for same-sex union only :
replace form_couple=3 if form_couple==1 & sex==sexcjt */

** For partners who immigrate from abroad between two years and have ///
** no information in t-1, we derive partnership formation from their partner's 
** information
** If two partners are immigrating in the same year we do not capture them
bysort codecouple year form_couple: gen problem=0 if codecouple!="" & ///
inlist(form_couple,1,2) & inlist(form_couple[_n+1],1,2)
bysort codecouple year: replace problem=1 if codecouple!="" & form_couple==0 ///
& inlist(form_couple[_n+1],1,2)
tab problem
// you can assume that these individuals form a new couple
replace form_couple=1 if problem==1
drop problem

** Rate of union formation
tab MS_AGE form_couple if year==1993, row nofreq m									// adjust to the selected year

**************************************************************
********* Creation of a variable of 1st marriage *************
**************************************************************

capture drop first_marr
bysort codern (year) : gen first_marr=1 if couple==1 & inlist(couple[_n-1],2,9) ///
& year==1993 & acheciv==1992 & CD_CIV==2 & CD_CIV[_n-1]==1 & codecouple!=""
replace first_marr=0 if first_marr==.

** 1st marriage rate
tab MS_AGE first_marr if year==1993, row nofreq m									// adjust to the selected year

compress
cd $outpath
save partnership_form.dta, replace







********************************************************************************
*** Exact date of the partnership formation using flow files *******************
********************************************************************************
** First, we need to clear the flow files and create information that sum up the 
** municipalities of origin and destination, and the type of moves.
** We will mainly work on the "t_2021_092_int_mig_inter_c.dta" file, and we will adding
** international emigration, immigration and intra-municipal moves
cd $inpath
use t_2021_092_int_mig_inter_c.dta, clear
gen tmig=1
save "/mnt/DEMO_SCHN/flow_internal.dta", replace

use t_2021_092_int_mig_intra_c.dta, clear
gen tmig=2
save "/mnt/DEMO_SCHN/flow_intra-m.dta", replace

use t_2021_092_tf_emigration_c.dta, clear
gen tmig=3
save "/mnt/DEMO_SCHN/flow_emigration.dta", replace

use t_2021_092_tf_immigration_c.dta, clear
gen tmig=4
save "/mnt/DEMO_SCHN/flow_immigration.dta", replace



use "/mnt/DEMO_SCHN/flow_internal.dta", clear
append  using "/mnt/DEMO_SCHN/flow_intra-m.dta"
append  using "/mnt/DEMO_SCHN/flow_emigration.dta"
append  using "/mnt/DEMO_SCHN/flow_immigration.dta"

lab def tmig 1"internal migration" 2"intramunicipal migration" 3"emigration (international)" 4"immigration (international)"
lab val tmig tmig

** NOTE : in 1992, no information about intra-municipal moves

** Commune of origin, for internal migration, emigration
gen commig_dep="" 
// Code of the municipality of departure if between municipality migration (internal)
replace commig_dep=CD_REFNIS if tmig==1
// code of the municipality of departue if international emigration.
replace commig_dep=CD_REFNIS if tmig==3


** Commune of destination, for internal migration, international immigration, 
** intracommunal migration
gen commig_arriv=""
replace commig_arriv=CD_REFNIS_D if tmig==1
replace commig_arriv=CD_REFNIS if tmig==4

** I label the variables
lab var commig_dep "Municipality of origin"
lab var commig_arriv "Municipality of destination"
lab var tmig "type of migration"

sort  ID_DEMO_C

** The date of the event is in day-month-year format
** I want to keep only the year to select the period of observation
gen amig=year(DT_REFDATE)
gen mmig=month(DT_REFDATE)
gen jmig=day(DT_REFDATE)
gen year=amig

** As we obtain all the information we want of the initial variables, 
** we may delete them
drop CD_REFNIS CD_REFNIS_D

*keep only the year of interest													// adjust to the selected year
keep if amig==1992

cd $outpath
save flow1992.dta, replace										

************************* Append the data *********************************
/* For more convenience and clarity between stock and flow files, we append the 
data rather than merge. It helps to rapidly distinguish the information that 
comes from the stock and those from the flow files with the type of moves. 

It means that we have 3 types of information for an individual between two years: 
*** a) The stock situation on the 1st of January 1991
*** b) The flow situation and all the moves between 1/1/91 & 1/1/92
*** c) The stock situation on the 1st of January 1992 
      (where we have the variable if there was a union formation during the year)
*/

use partnership_form.dta, clear
append using flow1992.dta

destring jnaiss, replace 

** We retrieve the time-invariant variables for the observations of the flow files
local CHAR cd_sex anaiss mnaiss jnaiss
foreach var of local CHAR {
bysort ID_DEMO_C (year CD_REL_HH_HD) : replace `var'=`var'[_n-1] if `var'==. & `var'[_n-1]!=.
}

*NB : Sex may change for transgender people that modify their sex at the civil state.
*NB : Some people appear only in the flow files, and not in the stock files 

*********** Creation of variables specific to the flow files
** Number and order of residential moves per year
capture drop nmigyear
bysort year ID_DEMO_C (mmig jmig) : gen nmigyear=_n if amig!=.

** Binary variable for a residential move btw two years
recode nmigyear (0=0) (1/15=1), gen(chgres)

** Apply to the first observation line of the year (the stock file at 01/01)
bysort ID_DEMO_C (year CD_REL_HH_HD) : replace chgres=1 if chgres[_n+1]==1

** NB : People with 3+ moves per year are very rare (<1%) --> 0.24% with FT dataset

****** Variable of position of moves for each individual during a year
** 1 = Situation on the 1/1/91
** 2 = Situation at the first move btw 91 and 92
** 3 = Situation at the 2nd move 
** ...
capture drop nobs
gen nobs=1 if CD_REL_HH_HD!=.
bysort year ID_DEMO_C (tmig nmigyear) : replace nobs=nmigyear+1 if nmigyear!=.

label define nobslab 1 "Stock 01/01" 2 "1st move" 3 "2nd move" 4 "3rd move" ///
5 "4th move" 6 "5th move" 7 "6th move" 8 "7th move" 9 "8th move" 10 "9th move"
label value nobs nobslab

******* Identify the last date of move and partnership formation
/* Date of partnership formation is considered to the last date of move 
** Multiple scenarios possible:
1) An individual is moving between two years and a couple formation is identified:
   select the last date of move for union formation
2) An individual is not moving between two years, yet forms a couple because
   a partner comes living in the household: retreive the information of the move 
   from the partner
3) Both individuals are moving between two years and a couple formation is 
   identified: when not moving at the same date, take the last move of a partner 
   to identify partnership formation
4) None of the individual is moving and still there is an union formation: 
   sometimes people are marrying and they were not identify as a couple in their 
   previous household, sometimes those cases may happen when someone is moving 
   out of the previous household and the new composition and n3link imply 
   the identification of an unmarried partner. It's a part of limitations of 
   the household configuration and partnership variable, which heavily rely 
   on the position occupied in the household.
*/

** Generate a variable with the date of the last move from previous year
capture drop date_last
bysort ID_DEMO_C (year nobs) : gen date_last=mdy(mmig[_n-1],jmig[_n-1],amig[_n-1]) ///
if mmig[_n-1]!=. & _N==_n
format date_last %tdDD.NN.YY

sort ID_DEMO_C year nobs

** Identifying the date of partnership formation
capture drop date_partner

** Situation 1)
bysort ID_DEMO_C (year nobs) : gen date_partner=mdy(mmig[_n-1],jmig[_n-1],amig[_n-1]) ///
if inlist(form_couple,1,2) & mmig[_n-1]!=. & ID_DEMO_C[_n-1]!=""

/* If you want to use separately month + day
bysort ID_DEMO_C (year nobs) : gen month_partner=mmig[_n-1] if inlist(form_couple,1,2) ///
& mmig[_n-1]!=. & ID_DEMO_C[_n-1]!=""
bysort ID_DEMO_C (year nobs) : gen day_partner=jmig[_n-1] if inlist(form_couple,1,2) ///
& jmig[_n-1]!=. & ID_DEMO_C[_n-1]!=""
*/

** Situation 2)
bysort year (codecouple date_partner) : replace date_partner=date_partner[_n-1] ///
if codecouple==codecouple[_n-1] & codecouple!="" & date_partner==.

/*
bysort year (codecouple month_partner) : replace month_partner=month_partner[_n-1] ///
if codecouple==codecouple[_n-1] & codecouple!="" & month_partner==.
bysort year (codecouple day_partner) : replace day_partner=day_partner[_n-1] ///
if codecouple==codecouple[_n-1] & codecouple!="" & day_partner==.
*/

** Situation 3)
bysort year (codecouple date_partner) : replace date_partner=date_partner[_n+1] ///
if codecouple==codecouple[_n+1] & codecouple!="" ///
& date_partner<date_partner[_n+1]
format date_partner %tdDD.NN.YY

bysort year (codecouple date_partner) : gen problem2=1 if codecouple==codecouple[_n+1] ///
& codecouple!="" & date_partner<date_partner[_n+1]

********************* Type of partnership formation, depending on who's moving :
capture drop migform
// The individual is moving to a partner's home
bysort year (codecouple) : gen migform=1 if codecouple==codecouple[_n+1] ///
& date_last!=. & date_last[_n+1]==. & inlist(form_couple,1,2)
bysort year (codecouple): replace migform=1 if codecouple==codecouple[_n-1] ///
& date_last!=. & date_last[_n-1]==. & inlist(form_couple,1,2)

// The partner is moving to the individual's home
bysort year (codecouple): replace migform=2 if codecouple==codecouple[_n+1] ///
& date_last==. & date_last[_n+1]!=. & inlist(form_couple,1,2)
bysort year (codecouple): replace migform=2 if codecouple==codecouple[_n-1] ///
& date_last==. & date_last[_n-1]!=. & inlist(form_couple,1,2)

// Both are moving into a new home during the year
bysort year (codecouple): replace migform=3 if codecouple==codecouple[_n+1] ///
& date_last!=. & date_last[_n+1]!=. & inlist(form_couple,1,2)
bysort year (codecouple): replace migform=3 if codecouple==codecouple[_n-1] ///
& date_last!=. & date_last[_n-1]!=. & inlist(form_couple,1,2)

// None are moving
bysort year (codecouple): replace migform=4 if codecouple==codecouple[_n+1] ///
& date_last==. & date_last[_n+1]==. & inlist(form_couple,1,2)
bysort year (codecouple): replace migform=4 if codecouple==codecouple[_n-1] ///
& date_last==. & date_last[_n-1]==. & inlist(form_couple,1,2)

// Both are moving during the year but not at the same date 
bysort year (codecouple): replace migform=5 if codecouple==codecouple[_n+1] ///
& date_last!=date_last[_n+1] & date_last[_n+1]!=. & date_last!=. & inlist(form_couple,1,2)
bysort year (codecouple): replace migform=5 if codecouple==codecouple[_n-1] ///
& date_last!=date_last[_n-1] & date_last[_n-1]!=. & date_last!=. & inlist(form_couple,1,2)

label define migformlab 1 "Ego moves" 2 "Partner moves" 3 "Both same date" ///
4 "No moves" 5 "Both no same date"
label value migform migformlab

label variable tmig "Type of migration"
label variable amig "Year of migration"
label variable mmig "Month of migration"
label variable jmig "Day of migration"
label variable commig_dep "Commune of origin"
label variable commig_arriv "Commune of destination"
label variable nmigyear "Order of move during a year"
label variable nobs "Order of the stock file + the move during a year"
label variable chgres "Binary variable move or not during a year"
label variable date_last "Date of the last migration (in t-1)"
label variable date_partner "Date of partnership formation"
label variable migform "Type of partnership formation (who's moving to whom)"

compress

** Number of days difference btw the two partners when moving during the year 
** but not at the same time
capture drop days_diff
bysort year (codecouple) : gen days_diff=abs(date_last-date_last[_n+1]) if migform==5
bysort year (codecouple) : replace days_diff=abs(date_last-date_last[_n-1]) ///
if migform==5 & days_diff==.

cd $outpath
save datepartnership_form.dta, replace
use datepartnership_form, clear

tab migform couple if migform!=., m


*************************** Union dissolution ****************************
** Same principles as for union formation 

** We just need to add the death information of the flow files
cd $inpath
use t_2021_092_tf_death_c.dta, clear
gen year=year(DT_REFDATE)
gen mmig=month(DT_REFDATE)
gen jmig=day(DT_REFDATE)
destring year, replace
gen date_death=mdy(mmig,jmig,year)
format date_death %tdDD.NN.YY
gen death=1
label variable death "Death during the year"
keep death date_death year ID_DEMO_C
keep if year==1992												// adjust to the selected year
cd $outpath
save Deces1992.dta, replace												// adjust to the selected year

** Creating a specific variable for people who die during the year 1991 and 
** adding it to the database
use datepartnership_form.dta, clear 
merge m:1 ID_DEMO_C year using Deces1992.dta
keep if inlist(_merge,1,3)
drop _merge

** Variable death partner 
capture drop death_partner
bysort year (codecouple) : gen death_partner=1 if death[_n+1]==1 & ///
codecouple==codecouple[_n+1] & codecouple!=""
bysort year (codecouple) : replace death_partner=1 if death[_n-1]==1 ///
& codecouple==codecouple[_n-1] & codecouple!=""

** Variable date of death partner 
capture drop datedeath_partner
bysort year (codecouple) : gen datedeath_partner=date_death[_n+1] if ///
death[_n+1]==1 & codecouple==codecouple[_n+1] & codecouple!=""
bysort year (codecouple) : replace datedeath_partner=date_death[_n-1] if ///
death[_n-1]==1 & codecouple==codecouple[_n-1] & codecouple!=""

format datedeath_partner %tdDD.NN.YY

****** Creation of the union dissolution variable (during previous year)
** If married or unmarried co-resident couple in t-1 (1991) and single or married LAT in t (1992)
capture drop end_couple
bysort nobs ID_DEMO_C (year) : gen end_couple=1 if inlist(couple[_n-1],1,2) ///
& inlist(couple,3,9) ///
& CD_REL_HH_HD!=. & CD_REL_HH_HD[_n-1]!=. & death_partner[_n-1]!=1

** Union dissolution and Repartnering from one year to another
bysort nobs ID_DEMO_C (year) : replace end_couple=2 if inlist(couple[_n-1],1,2) ///
& inlist(couple,1,2) & codecouple[_n-1]!=codecouple & codecouple!="" ///
& CD_REL_HH_HD!=. & CD_REL_HH_HD[_n-1]!=. & death_partner[_n-1]!=1

** Widowhood, single
bysort nobs ID_DEMO_C (year) : replace end_couple=3 if inlist(couple[_n-1],1,2) ///
& inlist(couple,3,9) & CD_REL_HH_HD!=. & CD_REL_HH_HD[_n-1]!=. & death_partner[_n-1]==1


** Widowhood, repartnered
bysort nobs ID_DEMO_C (year) : replace end_couple=4 if inlist(couple[_n-1],1,2) ///
& inlist(couple,1,2) & codecouple[_n-1]!=codecouple & codecouple!="" ///
& CD_REL_HH_HD!=. & CD_REL_HH_HD[_n-1]!=. & death_partner[_n-1]==1

replace end_couple=0 if end_couple==. & inlist(couple,1,2) & year==1993

label def endcouplelab 0 "No End" 1 "End, single" 2 "End, repartner" ///
3 "Widow, single" 4 "Widow,repartner"
label val end_couple endcouplelab


********** Type of partnership dissolution, depending on who's moving 
//(ONLY FOR SEPARATION, NOT WIDOWHOOD) :
/*The date of the union dissolution is the date of the first move of an ego or
his/her partner*/

** Create a codecouple from the last year to have information on moves of the ex-partner
bysort nobs ID_DEMO_C (year) : gen codecouplen1=codecouple[_n-1] 

** Create a variable for the move of the partner during the year
bysort ID_DEMO_C year (nobs) : gen date_first=mdy(mmig[_n+1],jmig[_n+1],year) ///
if nobs[_n+1]==2

format date_first %tdDD.NN.YY

label variable date_first "Date of the first migration (btw t and t+1)"

** Move of the partner btw t and t+1
capture drop move_partner
bysort year (codecouple) : gen move_partner=1 if date_first[_n+1]!=. & ///
codecouple==codecouple[_n+1] & codecouple!=""
bysort year (codecouple) : replace move_partner=1 if date_first[_n-1]!=. & ///
codecouple==codecouple[_n-1] & codecouple!=""

label variable move_partner "Move of the (ex-)partner between t and t+1 (yes/no)"

** Date of the first move of the partner btw t and t+1
capture drop datemove_partner
bysort year (codecouple) : gen datemove_partner=date_first[_n+1] if ///
date_first[_n+1]!=. & codecouple==codecouple[_n+1] & codecouple!=""
bysort year (codecouple) : replace datemove_partner=date_first[_n-1] if ///
date_first[_n-1]!=. & codecouple==codecouple[_n-1] & codecouple!=""

format datemove_partner %tdDD.NN.YY

label variable datemove_partner "Date of the first move of the (ex-)partner btw t and t+1"

capture drop migend

// The individual is moving out and the ex-partner stays in the home
bysort year (codecouplen1) : gen migend=1 if codecouplen1==codecouplen1[_n+1] ///
& date_last!=. & date_last[_n+1]==. & inlist(end_couple,1,2)
bysort year (codecouplen1) : replace migend=1 if codecouplen1==codecouplen1[_n-1] ///
& date_last!=. & date_last[_n-1]==. & inlist(end_couple,1,2)

// The ex-partner is moving out and the individual stays home
bysort year (codecouplen1) : replace migend=2 if codecouplen1==codecouplen1[_n+1] ///
& date_last==. & date_last[_n+1]!=. & inlist(end_couple,1,2)
bysort year (codecouplen1) : replace migend=2 if codecouplen1==codecouplen1[_n-1] ///
& date_last==. & date_last[_n-1]!=. & inlist(end_couple,1,2)

// Both are moving out into a new home during the year
bysort year (codecouplen1) : replace migend=3 if codecouplen1==codecouplen1[_n+1] ///
& date_last!=. & date_last[_n+1]!=. & inlist(end_couple,1,2)
bysort year (codecouplen1) : replace migend=3 if codecouplen1==codecouplen1[_n-1] ///
& date_last!=. & date_last[_n-1]!=. & inlist(end_couple,1,2)

// None are moving
bysort year (codecouplen1) : replace migend=4 if codecouplen1==codecouplen1[_n+1] ///
& date_last==. & date_last[_n+1]==. & inlist(end_couple,1,2)
bysort year (codecouplen1) : replace migend=4 if codecouplen1==codecouplen1[_n-1] ///
& date_last==. & date_last[_n-1]==. & inlist(end_couple,1,2)

// Both are moving out during the year but not at the same date 
bysort year (codecouplen1) : replace migend=5 if codecouplen1==codecouplen1[_n+1] ///
& date_last!=date_last[_n+1] & date_last[_n+1]!=. & date_last!=. & inlist(end_couple,1,2)
bysort year (codecouplen1) : replace migend=5 if codecouplen1==codecouplen1[_n-1] ///
& date_last!=date_last[_n-1] & date_last[_n-1]!=. & date_last!=. & inlist(end_couple,1,2)

tab migend if inlist(end_couple,1,2), m

************ For ex-partners not in the 1992 stock file
bysort nobs ID_DEMO_C (year) : replace migend=1 if inlist(end_couple,1,2) & ///
move_partner[_n-1]==. & date_last!=. & migend==.

bysort nobs ID_DEMO_C (year) : replace migend=2 if inlist(end_couple,1,2) & ///
move_partner[_n-1]==1 & date_last==. & migend==.

bysort nobs ID_DEMO_C (year) : replace migend=3 if inlist(end_couple,1,2) & ///
move_partner[_n-1]==1 & date_last!=. & datemove_partner[_n-1]==date_first[_n-1] ///
& migend==.

bysort nobs ID_DEMO_C (year) : replace migend=4 if inlist(end_couple,1,2) & ///
move_partner[_n-1]==. & date_last==. & migend==.

bysort nobs ID_DEMO_C (year) : replace migend=5 if inlist(end_couple,1,2) & ///
move_partner[_n-1]==1 & date_last!=. & datemove_partner[_n-1]!=date_first[_n-1] ///
& migend==.

label define migendlab 1 "Ego moves" 2 "Ex-Partner moves" 3 "Both same date" ///
4 "No moves" 5 "Both no same date"
label value migend migendlab

tab migend if inlist(end_couple,1,2), m

** None are moving : in most cases those this is an issue for complex households.
** More rare to move out at the exact date, because the registration depends on 
// the municipalities of destination, not the origins.

********** Date of partnership dissolution, no matter who moves
//(ONLY FOR SEPARATION, NOT WIDOWHOOD) :

capture drop date_diss
g date_diss = date_last if inlist(end_couple, 1, 2)
bysort ID_DEMO_C (year nobs): replace date_diss = datemove_partner[_n-1] if inlist(end_couple, 1, 2) & date_diss==. & date_last==.
format date_diss %tdDD.NN.YY


label variable end_couple "Partnership dissolution during previous year"
label variable date_diss "Date of partnership dissolution"
label variable death_partner "Death of the partner btw t and t+1"
label variable datedeath_partner "Date of death of the partner btw t and t+1"
label variable migend "Type of partnership dissolution (who's moving out)"

compress
cd $outpath
save datepartnership_form.dta, replace
use datepartnership_form.dta, clear


