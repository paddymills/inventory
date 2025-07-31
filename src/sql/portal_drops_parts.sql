select distinct
	ProgramName, PartName, QtyInProcess
from PIPArchive
where PartName like '1210027[BE]%'
	and TransType = 'SN102'
	and WONumber like '1210027[BE]-[27]-%'
