#' make full database from extract
#'
#' @export
make_fulldb <- function(dbFile,gPath,cPath,db=F,ow=F){
  if(db){
    dbFile <- file.Path(.locData,"gold_final.duckdb")
    gPath <- file.Path(.locData,"gold","extract")
    cPath <- file.Path(.locDir,"refdata","gold","202409_Lookups_CPRDGold")
  }else{
    if(ow && file.exists(dbFile)){
      unlink(dbFile)
    }
  }
  if(file.exists(gPath)){
    cprdgoldtools::load_patients(pdir=gPath,dbf=dbFile)
    cprdgoldtools::load_clinical(pddir=gPath,dbf=dbFile)
    cprdgoldtools::load_cons(pddir=gPath,dbf=dbFile)
    cprdgoldtools::load_additional(pddir=gPath,dbf=dbFile)
    cprdgoldtools::load_therapy(pddir=gPath,dbf=dbFile)
    cprdgoldtools::load_practice(pddir=gPath,dbf=dbFile)
    cprdgoldtools::load_immunisation(pddir=gPath,dbf=dbFile)
    cprdgoldtools::load_staff(pddir=gPath,dbf=dbFile)
    cprdgoldtools::load_referrals(pddir=gPath,dbf=dbFile)
    cprdgoldtools::load_test(pddir=gPath,dbf=dbFile)
    cprdgoldtools::load_cprdfiles(pddir=cPath,dbf=dbFile)
  }
  return(dbFile)
}
