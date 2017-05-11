library(RODBC)
svr <- "server=VSQL08\\HYPERLOOP;"
db <- "database=ArtificialIntelligence;"
uid <- "uid=sa;"
pwd <- "pwd=mttuvxz"
conns <- paste0("driver={ODBC Driver 13 for SQL Server};",
                svr, db, uid, pwd)
cache_folder = "c:/_hyperloop_data/powerbi_scripts"
dir.create(cache_folder, recursive = TRUE, showWarnings = FALSE)
cache_file = paste0(cache_folder, "/temp_clusters.csv")

strTbl <- "


SELECT [SGM_ID]
,[SGM_DET_ID]
,[PER_ID]
,[SGM_NO]
,[MICRO_SGM_NO]
,[MICRO_SGM_ID]
,[LABEL]
,[x_tSNE]
,[y_tSNE]
,[R]
,[F]
,[M]
,[SALES]
,[MARGIN]
,[BILL]
,[BILLAVG]
,[VVPAM]
,[VALDOC]
,[LOC]
,[LOCD]
,[TOPLOC1]
,[TOPLOC2]
,[TOPLOC3]
,[NETSALES]
,[Age]
,[Churn]
,[TOPLOC1_TRANCNT]
,[PRODAVG]
,[BRAND_GENERAL]
,[DR_HART]
,[NUROFEN]
,[APIVITA]
,[AVENE]
,[L_OCCITANE]
,[VICHY]
,[BIODERMA]
,[LA_ROCHE_POSAY]
,[L_ERBOLARIO]
,[PARASINUS]
,[TRUSSA]
,[SORTIS]
,[NESTLE]
,[OXYANCE]
,[TERTENSIF]
,[ASPENTER]
,[ALPHA]
,[BATISTE]
,[STREPSILS]
,[PHARMA]
,[COSMETICE]
,[DERMOCOSMETICE]
,[BABY]
,[NEASOCIATE]
,[IT_PHARMA]
,[IT_NON_PHARMA]
,[RX]
,[NON_RX]
,[MA_NonDCI]
,[B_D_C_NonBPS]
,[FaraCategorie]
,[B_D_C_BPS]
,[RX_NonCNAS]
,[OTC_MA_DCI]
,[MedicalPrescription]
,[WithoutPrescription]
,[Oncology]
FROM [dbo].[VW_SGM_MICRO]
where [SGM_ID]=39"
 
 
#channel <- odbcDriverConnect(conn)
if(! file.exists(cache_file))
{
  conn<-odbcDriverConnect(conns)
  dataset <- sqlQuery(conn, paste(strTbl)) 
  write.csv(dataset,file = cache_file, row.names = FALSE)
  close(conn)
}else{
  dataset <- read.csv(cache_file)
}

 
 


# script R pentru plotarea segmentarii semantice in PowerBI
# textele care incep cu # sunt comentarii

library(ggplot2)
VER <- "0.1.2"

# se presupune ca dataframe-ul are field-urile tsneX, tsneY precum si cate un field 
#  pentru toate CA-uri la categoriile si toate brand-urile

# in loc de x_tSNE intre ghilimele se pune numele campului pentru coordonata X a tSNE-ului
# DACA numele de camp este altul !
tsneX_FIELD <- "x_tSNE"  

# in loc de y_tSNE intre ghilimele se pune numele campului pentru coordonata Y a tSNE-ului
# DACA numele de camp este altul !
tsneY_FIELD <- "y_tSNE"  

# in loc de SGM_NO intre ghilimele se pune numele campului pentru ID-ul de cluster (1-4)
# DACA numele de camp este altul !
segment_FIELD <- "SGM_NO" 

# in loc de MICRO_SGM_NO intre ghilimele se pune numele campului pentru ID-ul de microcluster
# DACA numele de camp este altul !
microsegment_FIELD <- "MICRO_SGM_NO" 


# in loc de textele intre ghilimele se pun numele campurilor categoriilor (CA-urile la nivel de microsegment)
# atentie ca numele campurilor sa fie scrise corect !
ALL_CATEGS_FIELDS <- c("PHARMA", "COSMETICE", "DERMOCOSMETICE", "BABY", "NEASOCIATE")


# in loc de textele intre ghilimele se pun numele campurilor brandurilor (CA-urile la nivel de microsegment)
# atentie ca numele campurilor sa fie scrise corect !
ALL_BRANDS_FIELDS <- c("DR_HART", "NUROFEN", "APIVITA", "AVENE", "L_OCCITANE", "VICHY",
                "BIODERMA", "LA_ROCHE_POSAY", "L_ERBOLARIO", "PARASINUS", "TRUSSA", "SORTIS", "NESTLE",
                "OXYANCE", "TERTENSIF", "ASPENTER", "ALPHA")


# configurare plot

SHAPE_SIZE <- 3 # dimensiunea formelor microsegm: 1 este foarte mic iar 5 deja este prea mare
FONT_SIZE <- 1.5 # marimea fontului cu care este scris TOPBRAND/TOPCATEG

### partea de sus se configureaza
### mai jos se lasa nemodificat


for (i in 1:nrow(dataset)) {
  best_categ1_index <- which.max(dataset[i, ALL_BRANDS_FIELDS])
  best_categ1 <- colnames(dataset[i, ALL_BRANDS_FIELDS])[best_categ1_index]
  best_categ2_index <- which.max(dataset[i, ALL_CATEGS_FIELDS])
  best_categ2 <- colnames(dataset[i, ALL_CATEGS_FIELDS])[best_categ2_index]
  dataset[i, "BESTOF"] <- paste0(best_categ1, "/", best_categ2)
}


tsne_colors <- as.factor(dfnew[,segment_FIELD])

plot2 <- qplot(dfnew[, tsneX_FIELD], dfnew[, tsneY_FIELD])


#maxFontSize = 10 
#FigWidth = 7.5 # cm 
#FigHeight = 7.5 # cm 
#inch = 2.54 # cm 
## tweek your screen so  that you got the right plot size in cm 
#windows.options(xpinch = 121, ypinch = 121)  
#windows(width=FigWidth/inch, height=FigWidth/inch, 
#        pointsize=maxFontSize) 
#plot2 <- plot2 + theme(plot.margin = unit(c(0, 0, 0, 0), "cm"))
        
plot2 <- plot2 + geom_point(aes(shape = tsne_colors, color = tsne_colors), 
                                    size = SHAPE_SIZE)
plot2 <- plot2 + geom_point(aes(alpha = 0.5))

plot2 <- plot2 + geom_text(aes(label = dfnew$BESTOF),
                          color = "black", size = FONT_SIZE)
plot2 <- plot2 + theme(legend.position = "none")

plot2