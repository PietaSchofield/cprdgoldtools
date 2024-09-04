#' load patients
#'
#' get the patient records and convert some fields to useful field types
#'
#' @param batch the name of the batch subdirectory
#' @param datadir the name of the data directory
#'
#' @export
load_patients <- function(pdir,dbf,ow=F,db=F,tab_name="patients",add=F,
   selvars1=c("patid","vmid","gender","yob","mob","marital","famnum","CHSreg","CHSdate","prescr",
              "capsup","frd","crd","regstat","reggap","internal","tod","toreason","deathdate","accept")){
  if(db){
    db <- T
    tab_name <- "patients"
    ow <- F
    dbf <- sgdb  
    pdir <- gpath
    add <- T
   selvars1 <- c("patid","vmid","gender","yob","mob","marital","famnum","CHSreg","CHSdate","prescr",
              "capsup","frd","crd","regstat","reggap","internal","tod","toreason","deathdate","accept")
  }
  if(add & ow){
    stop("Error both overwrite and append set. \n")
    return()
  }
  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  tabs <- duckdb::dbListTables(dbi)
  duckdb::dbDisconnect(dbi)
  if(!tab_name%in%tabs || ow || add){
    patfiles <- list.files(pdir,pattern="Patient",full=T,recur=T)
    patfiles
    dat <- lapply(patfiles,function(fn){
       readr::read_tsv(fn,col_type=readr::cols(.default=readr::col_character())) %>%
                     dplyr::select(all_of(selvars1)) %>%
                     dplyr::mutate(yob = as.integer(yob),
                            CHSdate = format(lubridate::dmy(CHSdate)),
                            frd = format(lubridate::dmy(frd)),
                            crd = format(lubridate::dmy(crd)),
                            tod = format(lubridate::dmy(tod)),
                            deathdate=format(lubridate::dmy(deathdate)))
                     }) %>% bind_rows()
    nrec <- dat %>% nrow()
    if(db) dat %>% dplyr::filter(!is.na(tod)) %>% head() %>% display_data(disp=T)
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf,write=T)
    duckdb::dbWriteTable(dbi,tab_name,dat,overwrite=ow,append=add)
    dbDisconnect(dbi)
    rm(dat)
    gc()
    ext <- "new records"
  }else{
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    nrec <- dbGetQuery(dbi,paste0("SELECT COUNT(*) AS patients FROM ",tab_name,";")) %>%
      pull(patients)
    dbDisconnect(dbi)
    ext <- "records exist"
  }
  trec <- sum(unlist(nrec))
  cat(paste0(trec," ",ext,"\n"))
  return(nrec)
}
