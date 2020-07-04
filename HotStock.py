# -*- coding: utf-8 -*-
"""
Created on Fri May 15 22:18:22 2020

@author: wudi
"""
import pandas as pd
from MSSQL import MSSQL
from Toolbox import DataCollect
from Toolbox import WeightScheme
from Toolbox import DataStructuring
from Toolbox import ReturnCal

WS=WeightScheme()
DC=DataCollect()
DS=DataStructuring()
RC=ReturnCal()
ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="jyzb_new_1") #This is PROD    

class QueryMgmt():
    def __init__(self):
        pass
    
        #PastNdays recommended stocks with no tickerlist
    def Hotstock_query(self,rebalday,lookback_period):
        sqlpart="Select '"+rebalday+"' as date, code, count(*) Reccount from jyzb_new_1.dbo.cmb_report_research R left join jyzb_new_1.dbo.I_SYS_CLASS C on C.SYS_CLASS=R.score_id left join jyzb_new_1.dbo.I_ORGAN_SCORE S on S.ID=R.organ_score_id where into_date>=dateadd(day,-"+str(lookback_period)+",'"+rebalday+"') and into_date<'"+rebalday+"' and (sys_class=7 OR sys_class=5) GROUP BY code ORDER BY Reccount DESC "
        return(sqlpart)
        
        
class Prep():
    def __init__(self):
        self.Q=QueryMgmt()
    
    #Construct the query of hotstock across different rebalday
    def Hotstock_nonsectorQuery(self,rebaldaylist,lookback_period):
        sql=[]
        for rebalday in rebaldaylist:
            sqlpart=self.Q.Hotstock_query(rebalday,lookback_period)
            sql.append(sqlpart)
        reslist=ms.ExecListQuery(sql)
        rechist=pd.DataFrame(reslist,columns=['date','ticker','raccount'])
        return(rechist)
    
    
    #Construct the query of hotstocks of a sector across different rebalday
    def Hotsotck_sectorQuery(self,rebaldaylist,lookback_period,df):
        sql=[]
        for rebalday in rebaldaylist:
            sqlpart=self.Q.Hotstock_query(rebalday,lookback_period)
            sql.append(sqlpart)
        reslist=ms.ExecListQuery(sql)
        rechist=pd.DataFrame(reslist,columns=['date','ticker','raccount'])
        rechist_sector=pd.DataFrame(columns=['date','ticker','raccount'])
        for rebalday in rebaldaylist:
            tickerlist=df.loc[df['date']==rebalday,'ticker'].tolist()
            rechist_rebalday=rechist.loc[(rechist['date']==rebalday)&(rechist['ticker'].isin(tickerlist)),:]
            rechist_sector=rechist_sector.append(rechist_rebalday)
        return(rechist_sector)
    
    #Select top xstocks stocks from a df using colindex
    def Top_stocks(self,df,colindex,topx,allnot):
        rebaldaylist=df['date'].unique()
        dfnew=pd.DataFrame(columns=df.columns)
        df[colindex]=df[colindex].astype(float)
        for rebalday in rebaldaylist:
            if allnot=='all':
                dfrebalday=df.loc[df['date']==rebalday,:].nlargest(topx,colindex,keep='all')
            else:
                dfrebalday=df.loc[df['date']==rebalday,:].nlargest(topx,colindex)
            dfnew=dfnew.append(dfrebalday)
        return(dfnew)
        
        
class StockPick():
    def __init__(self):
        self.P=Prep()
     
    #startdate='2015-12-28',benchmark='CSI300', rebal_period=10,lookback_period=30, primecodelist=[40,36],topx=60
    #Choose a benchmark, return top xstocks of most recommended stocks (OF a sector) among benchmark memb stocks
    #p=SP.Rec_stat_benchmark(startdate,benchmark,rebal_period,topx,lookback_period,'N')
    #p=SP.Rec_stat_benchmark(startdate,benchmark,rebal_period,topx,lookback_period,[40,36])
    def Rec_stat(self,dailyreturn,rebaldaylist,lookback_period,primecodelist,publisher):
        if primecodelist=='N':
            rechist=self.P.Hotstock_nonsectorQuery(rebaldaylist,lookback_period)
        else:
            sectorstock=DC.Sector_stock(rebaldaylist,primecodelist,publisher)
            rechist=self.P.Hotsotck_sectorQuery(rebaldaylist,lookback_period,sectorstock)
        rechist_active=WS.Active_stock_screening(rechist,dailyreturn,rebaldaylist)
        return(rechist_active)
    
    #postab=SP.Rec_stat_benchmark(dailyreturn,startdate,'CSI300',70,10,30,'N')
    def Rec_stat_benchmark(self,dailyreturn,benchmark,topx,lookback_period,primecodelist,rebaldaylist,publisher):
        rechist_active=self.Rec_stat(dailyreturn,rebaldaylist,lookback_period,primecodelist,publisher)
        rechist_bm=WS.Benchmark_intersect(rechist_active,benchmark)
        rechist_bm=rechist_bm.sort_values(['date','raccount','weight'],ascending=[True,False,False])
        rechist_selected=self.P.Top_stocks(rechist_bm,'raccount',topx,'notall')
        postab=WS.Generate_PortNav(rechist_selected)
        return(postab)
    
    def Rec_stat_nonbenchmark(self,dailyreturn,topx,lookback_period,primecodelist,rebaldaylist,publisher):
        rechist_active=self.Rec_stat(dailyreturn,rebaldaylist,lookback_period,primecodelist,publisher)
        mcaphist=DC.Mcap_hist(rebaldaylist,rechist_active)
        rechist_mcap=DS.Data_merge(rechist_active,mcaphist,'mcap')
        rechist_mcap=rechist_mcap.sort_values(['date','raccount','mcap'],ascending=[True,False,False])
        rechist_selected=self.P.Top_stocks(rechist_mcap,'raccount',topx,'notall')
        rechist_selected=rechist_selected.rename(columns={'mcap':'weight'})
        postab=WS.Generate_PortNav(rechist_selected)
        return(postab)

class SecR():
    def __init__(self):
        self.P=Prep()
        self.SP=StockPick()
    
    def SecStats(self,dailyreturn,rebaldaylist,lookback_period):
        #rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
        rechist=self.P.Hotstock_nonsectorQuery(rebaldaylist,lookback_period)
        rechist_active=WS.Active_stock_screening(rechist,dailyreturn,rebaldaylist)
        tickerlist=rechist_active['ticker'].unique()
        stock_sector=DC.Stock_sector(rebaldaylist,tickerlist,'CITIC')
        totalsecnum=len(stock_sector['primecode'].unique())
        selectsecsnum=int(totalsecnum*0.3)
        rechist_active,stock_sector=map(DS.Addindex,(rechist_active,stock_sector))
        rechist_active_sector=pd.merge(rechist_active,stock_sector[['index','primecode']],on='index',how='left')
        rechist_active_sector['primindex']=rechist_active_sector['date']+rechist_active_sector['primecode']
        secrac=pd.DataFrame(rechist_active_sector.groupby(['primindex'])['raccount'].sum())
        secrac.reset_index(inplace=True)
        secrac['date']=secrac['primindex'].str[0:10]
        secrac['sector']=secrac['primindex'].str[10:13]
        for rebalday in rebaldaylist:
            secrac.loc[secrac['date']==rebalday,'rank'] = secrac.loc[secrac['date']==rebalday,'raccount'].rank(ascending=False,method='min')
        topsec=secrac.loc[secrac['rank']<=selectsecsnum]
        return(topsec)
    
    def Getsecname(self,topsec):
        sec_name=DC.Sec_name('CITIC')
        sec_name['sectorname']=[x.encode('latin-1').decode('gbk') for x in sec_name['sectorname']]
        sec_name=sec_name.drop_duplicates(['sector'],keep="first")
        topsec=pd.merge(topsec,sec_name,how='left',on='sector')
        topsec=topsec[['date','sector','sectorname','raccount','rank']]
        #topsec=topsec.sort_values(by=['rank'])
        return(topsec)
        
class Review():
    def __init__(self):
        self.P=Prep()
        self.SP=StockPick()
    
    #To produce the Hotstock postab on weekly basis according to Wind sector categorization, produce the whole holding history since 2005-12-28 every week
    def Postab_ProductionSec(self,dailyreturn,rebaldaylist):
        sector_summary=pd.read_csv("D:/SecR/Sector_summary.csv")
        sector2=sector_summary[['NewWind','HighReturn']].drop_duplicates(inplace=False) #Mapping of Windsector with Prime Industry of Citics
        postabhist=pd.DataFrame(columns=['date','ticker','PortNav%'])
        for windsector in sector2['NewWind']:
            print(windsector)
            topx=int(sector2.loc[sector2['NewWind']==windsector,'HighReturn'])
            primseclist=list(sector_summary.loc[sector_summary['NewWind']==windsector,'PrimSecCode'].astype(str))
            postab=self.SP.Rec_stat_benchmark(dailyreturn,'CSI800',topx,10,primseclist,rebaldaylist,'CITIC')
            postabhist=postabhist.append(postab)
            postab.to_csv("D:/Hotstocks/"+windsector+".csv",index=False)
        return(postabhist)
        
    #To produce the Hotstock postab on daily basis, with benchmark 
    def ActivepickBM_production(self,dailyreturn,bm,topx,rebaldaylist):
        postab=self.SP.Rec_stat_benchmark(dailyreturn,bm,topx,30,'N',rebaldaylist,'CITIC')
        postab.to_csv("D:/Hotstocks/Hotstock_"+bm+".csv",index=False)
        return(postab)
    
    #Pick the top30% of EVERY CSI sector, AFTER OR NOT intersection with a BM.
    def ActivepickBMSec_production(self,dailyreturn,bm,rebaldaylist):
        rechist_active=self.SP.Rec_stat(dailyreturn,rebaldaylist,60,'N','CSI')
        #rechist_bm=WS.Benchmark_intersect(rechist_active,bm)       #No intersection with BM
        rechist_bm=rechist_active.copy()
        mcaphist=dailyreturn.loc[(dailyreturn['date'].isin(rechist_bm['date'].unique()))&(dailyreturn['ticker'].isin(rechist_bm['ticker'].unique())),['date','ticker','mcap']]
        rechist_mcap=DS.Data_merge(rechist_bm,mcaphist,'mcap')
        tickerlist=rechist_mcap['ticker'].unique().tolist()
        stock_sector=DC.Stock_sector(rebaldaylist,tickerlist,'CSI')
        rechist_mcap,stock_sector=map(DS.Addindex,(rechist_mcap,stock_sector))
        rechist_mcap=pd.merge(rechist_mcap,stock_sector[['index','primecode']],on='index',how='left')
        rechist_mcap['index']=rechist_mcap['date']+rechist_mcap['primecode']
        rechist_mcap=rechist_mcap.sort_values(by=['date','raccount','mcap'],ascending=[True,False,False])
        rechist_mcap['nthoccur']=rechist_mcap.groupby('index').cumcount()+1
        indexcounts=pd.DataFrame(rechist_mcap['index'].value_counts())
        indexcounts.reset_index(inplace=True)
        indexcounts.columns=['index','totaloccur']
        rechist_mcap=pd.merge(rechist_mcap,indexcounts,on='index',how='left')
        rechist_mcap['diff']=rechist_mcap['nthoccur']-rechist_mcap['totaloccur']*0.3
        rechist_mcap=rechist_mcap.loc[rechist_mcap['diff']<=0,:]
        rechist_mcap=rechist_mcap[['date','ticker','raccount']]
        return(rechist_mcap)
    
    #Produce absolute top topx stocks (with no %PortNav) on each rebalday, no intersection, marketcap or sector 
    def ActivepickNS_production(self,startdate,rebal_period,dailyreturn,topx):
        rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
        rechist_active=self.SP.Rec_stat(dailyreturn,rebaldaylist,30,'N','CITIC')
        activepickhist=self.P.Top_stocks(rechist_active,'raccount',topx,'all')
        return(activepickhist)
    
    #Produce topx picks of each sector on every rebalday according to sector summary table with NO PortNAV%, no intersection and no marketcap
    def ActivepickSec_production(self,startdate,rebal_period,dailyreturn):
        rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
        sector_summary=pd.read_csv("D:/SecR/Sector_summary.csv")
        sector2=sector_summary[['NewWind','HighReturn']].drop_duplicates(inplace=False) #Mapping of Windsector with Prime Industry of Citics
        activepickhist=pd.DataFrame(columns=['date','ticker','raccount','mcap'])
        for windsector in sector2['NewWind']:
            print(windsector)
            topx=int(sector2.loc[sector2['NewWind']==windsector,'HighReturn'])
            primseclist=list(sector_summary.loc[sector_summary['NewWind']==windsector,'PrimSecCode'].astype(str))
            rechist_active=self.SP.Rec_stat(dailyreturn,rebaldaylist,30,primseclist)
            mcaphist=DC.Mcap_hist(rebaldaylist,rechist_active)
            rechist_mcap=DS.Data_merge(rechist_active,mcaphist,'mcap')
            rechist_mcap=rechist_mcap.sort_values(['date','raccount','mcap'],ascending=[True,False,False])
            rechist_selected=self.P.Top_stocks(rechist_mcap,'raccount',topx,'notall')
            activepickhist=activepickhist.append(rechist_selected)
        return(activepickhist)
                    
            
    #Compare the sector strategy return and the return of sector in BM
    #dailyreturn=pd.read_csv("U:/S/SecR/DailyReturn.csv")
    #dailyreturn['ticker']=[x[0:6]for x in dailyreturn['ticker']]
    def SectorPNLvsBM(self,dailyreturn,startdate,benchmark,topx,rebal_period,lookback_period,primecodelist):
        postabStrat=self.SP.Rec_stat_benchmark(dailyreturn,startdate,benchmark,topx,rebal_period,lookback_period,primecodelist)
        comptab=RC.SectorPNLvsBM(dailyreturn,postabStrat,benchmark,primecodelist)
        return(comptab)
    
    #startdate='2015-12-28'
    #topx=70
    #rebal_period=10
    #lookback_period=30
    #primecodelist='N'
    #benchmark='CSI300'
    def PNLCal(self,dailyreturn,startdate,topx,rebal_period,lookback_period,benchmark,primecodelist):
        postab=self.SP.Rec_stat_benchmark(dailyreturn,startdate,benchmark,topx,rebal_period,lookback_period,primecodelist)
        SPNL=RC.DailyPNL(dailyreturn,postab)
        CumPNL=RC.CumPNL(SPNL)
        return(CumPNL)
    