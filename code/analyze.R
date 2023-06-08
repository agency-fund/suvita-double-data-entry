library('data.table')
library('googlesheets4')

# numerous possibilities are not covered here

# manual steps:
# - created google cloud project
# - enabled google drive api and google sheets api
# - created service account
# - created a new key (json) for the service account
# - added email address in google token as editor to google sheet
# - added google token json as Secret for GitHub Actions

# authenticate
if (Sys.getenv('GOOGLE_TOKEN') == '') {
  gs4_auth()
} else {
  gs4_auth(path = Sys.getenv('GOOGLE_TOKEN'))
}

# load parameters
params = yaml::read_yaml('params.yaml')
gsheet_id = as_sheets_id(params$gsheet_url)

# read data from google sheet
data_entry = setDT(read_sheet(gsheet_id, params$wsheet_form_entry))
data_recon = setDT(read_sheet(gsheet_id, params$wsheet_form_recon))

# make sure only most recent response for each record ID and submitter
setorderv(data_entry, 'Timestamp')
data_entry = data_entry[, .SD[.N], keyby = c('Record ID', 'Email Address')]

setorderv(data_recon, 'Timestamp')
data_recon = data_recon[, .SD[.N], keyby = c('Record ID', 'Email Address')]

# figure out which columns come from data entry
entry_cols = setdiff(
  colnames(data_entry), c('Timestamp', 'Record ID', 'Email Address'))

# get responses for records having any discrepancies
data_discrepant_all = data_entry[
  , if (uniqueN(.SD, entry_cols) > 1L) .SD, keyby = 'Record ID']

# get discrepant responses that have not been resolved,
# i.e., record ids not in the reconciliation data
data_discrepant_unrecon = data_discrepant_all[!data_recon, on = 'Record ID']

# get clean data consisting of records that have been reconciled
# and records that did not need to be reconciled
data_clean = rbind(
  data_recon[
    , c('Record ID', entry_cols), with = FALSE][, was_discrepant := TRUE],
  unique(data_entry[
    !data_discrepant_all, c('Record ID', entry_cols), with = FALSE])[
      , was_discrepant := FALSE])
setkeyv(data_clean, 'Record ID')

write_sheet(data_discrepant_all, gsheet_id, 'discrepancies_all')
write_sheet(data_discrepant_unrecon, gsheet_id, 'discrepancies_unreconciled')
write_sheet(data_clean, gsheet_id, 'records_clean')
