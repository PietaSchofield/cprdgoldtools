# get_drugissues_batch
#'
#' get the drugissue records and convert some fields to useful field types
#'
#' @param batch the name of the batch subdirectory
#' @param datadir the name of the data directory
#' @param bpp BiocParallal Multicore Parameters
#'
#' @export
load_clinical <- function(pddir,dbf,ow=F,db=F,tab_name="clinical",add=F,
    selvars2=c("patid","eventdate","sysdate","constype","consid","medcode","sctid","sctdescid",
                  "sctexpression","sctmaptype","sctmapversion","sctisindicative","sctisassured",
                  "staffid","episode","enttype","adid")){
  if(F){
    pddir <- gpath
    dbf <- sgdb
    ow <- F
    db <- F
    add <- T
    tab_name <- "clinical"
    selvars2 <- c("patid","eventdate","sysdate","constype","consid","medcode","sctid","sctdescid",
                  "sctexpression","sctmaptype","sctmapversion","sctisindicative","sctisassured",
                  "staffid","episode","enttype","adid")
  }
  if(ow && add){
    stop("Error both overwrite and append true\n")
    return()
  }
  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  tabs <- dbListTables(dbi)
  duckdb::dbDisconnect(dbi)  
  nrec <- 0
  if(!tab_name%in%tabs || ow || add){
    difiles <- list.files(pddir,pattern="Clinical",full=T,recur=T)
    if(tab_name%in%tabs && ow){
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      DBI::dbExecute(dbi,paste0("DROP TABLE ",tab_name,";"))
      duckdb::dbDisconnect(dbi)  
    }
    nrec <- lapply(difiles,function(fn){
      dat <- readr::read_tsv(fn,col_types=readr::cols(.default=readr::col_character())) %>%
        dplyr::select(dplyr::all_of(selvars2)) %>%
        dplyr::mutate(eventdate=format(lubridate::dmy(eventdate)),
               sysdate=format(lubridate::dmy(sysdate)))
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      duckdb::dbWriteTable(dbi,tab_name,dat,append=T)
      duckdb::dbDisconnect(dbi)  
      nr <- dat %>% nrow()
      cat(paste0(fn," ",nr," records loaded\n"))
      rm(dat)
      gc()
    })
    ext <- "new records"
  }else{
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    nrec <- dbGetQuery(dbi,paste0("SELECT COUNT(*) FROM ",tab_name,";"))
    dbDisconnect(dbi)
    ext <- "records exist"
  }
  trec <- sum(unlist(nrec))
  return(cat(paste0(trec," ",ext,"\n")))
}
