--username lb2 --password "X0xCMnNxbHNlcnZlcjIwMDg=" --database master --port 1433 --query "select+count%28%2A%29+from+sys.databases+where+state_desc%3C%3E%27ONLINE%27" --result "0" --decode --warning 50 --critical 200 --querywarning 3 --querycritical 7 --result 0
/* NOTE:
Before reducing the max server memory value, use Performance Monitor to examine the SQLServer:Buffer Manager performance object while under a load, 
and note the current values of the Stolen pages and Reserved pages counters. These counters report memory as the number of 8K pages. max server 
memory should be set above the sum of these two values to avoid out-of-memory errors. An approximate value for the lowest reasonable max server memory
 setting (in MB) is ([Stolen pages] + [Reserved pages])/ 100. To reduce the max server memory you may need to restart SQL Server to release the memory.
 For information about how to set memory options, see How to: Set a Fixed Amount of Memory (SQL Server Management Studio).
*/
--GET ALL PARAMETERS;
exec sp_configure;

--GET DATABASE FILES AND SIZES
SELECT db.database_id,db.name as db_name, db.recovery_model_desc, mf.name as file_name,mf.type_desc, mf.physical_name  AS current_file_location,size*8/1024 AS SIZE_MB
FROM sys.master_files mf
INNER JOIN sys.databases db on mf.database_id=db.database_id;

--GET DATABASES AND STATUS
select database_id,name,create_date,recovery_model_desc,user_access_desc,state_desc from sys.databases;

--GET ALL SESSIONS
exec sp_who;

--GET ALL SESSIONS WITH WAITS
SELECT sess.session_id,sess.host_name,sess.program_name,sess.login_name, waits.wait_duration_ms/1000 AS seconds_in_wait, waits.wait_type, waits.blocking_session_id 
FROM sys.dm_exec_sessions sess left join sys.dm_os_waiting_tasks waits on (sess.session_id=waits.session_id)
where sess.host_name is not null
order by seconds_in_wait desc;

--GET ALL ACTIVE SESSIONS WITH COMMAND AND OTHER DETAILS
select sp.spid,sp.login_time,sp.status,sp.hostname,sp.program_name,sp.hostprocess,sp.blocked,
sp.open_tran,er.cpu_time,er.total_elapsed_time/1000/60 as elapsed_minutes,sp.waittype,er.wait_type,
sp.waittime,db.name as db_name,su.name as username,sp.cmd,sp.loginame, sqltext.text
from sys.sysprocesses sp
INNER JOIN sys.databases db on sp.dbid=db.database_id
INNER JOIN sys.sysusers su on sp.uid = su.uid
LEFT OUTER JOIN sys.dm_exec_requests er on sp.spid=er.session_id
OUTER APPLY sys.dm_exec_sql_text(er.sql_handle) AS sqltext
where sp.status<>'sleeping';
--and sp.spid in (156)
--and db.name='ATS';

--KILL SESSION
KILL 493;

--GET COMMAND FROM A SESSION:
DECLARE @sqltext VARBINARY(128)
SELECT @sqltext = sql_handle
FROM sys.sysprocesses
WHERE spid = 76
SELECT TEXT
FROM sys.dm_exec_sql_text(@sqltext)
GO

--KILL ALL SESSIONS FROM ONE DATABASE AND ONE USER
DECLARE @DbName nvarchar(50)
SET @DbName = N'DATABASENAME'
 
DECLARE @EXECSQL varchar(max)
SET @EXECSQL = ''
 
SELECT @EXECSQL = @EXECSQL + 'Kill ' + Convert(varchar, SPId) + ';'
FROM SysProcesses
WHERE DBId = DB_ID(@DbName) and loginame=N'USERNAME'
print @EXECSQL
--exec(@EXECSQL)

--GET LOCKS
SELECT DISTINCT name AS database_name, session_id, host_name, login_time, login_name, status, reads, writes
FROM    sys.dm_exec_sessions
        LEFT OUTER JOIN sys.dm_tran_locks ON sys.dm_exec_sessions.session_id = sys.dm_tran_locks.request_session_id
        INNER JOIN sys.databases ON sys.dm_tran_locks.resource_database_id = sys.databases.database_id
WHERE   resource_type <> 'DATABASE'
--AND name ='YourDatabaseNameHere'
ORDER BY name;

--MORE LOCKS
SELECT session_id, wait_duration_ms/1000 AS seconds_in_wait, wait_type, blocking_session_id 
FROM sys.dm_os_waiting_tasks 
WHERE blocking_session_id <> 0
and wait_type<>'CXPACKET';


--GET BACKUPS
SELECT sdb.Name AS DatabaseName,
COALESCE(CONVERT(VARCHAR(12), MAX(bus.backup_finish_date), 101),'-') AS LastBackUpTime
FROM sys.sysdatabases sdb
LEFT OUTER JOIN msdb.dbo.backupset bus ON bus.database_name = sdb.name
GROUP BY sdb.Name;

--BACKUP BY DATABASE, TYPE, SIZE AND TIME
SELECT  
   CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server, msdb.dbo.backupset.database_name, 
   msdb.dbo.backupset.backup_start_date, msdb.dbo.backupset.backup_finish_date, msdb.dbo.backupset.expiration_date, 
   CASE msdb..backupset.type  
       WHEN 'D' THEN 'Database'  
       WHEN 'L' THEN 'Log'  
       WHEN 'I' THEN 'Incremental' 
   END AS backup_type,  
   round(msdb.dbo.backupset.backup_size/1024/1024,2) as Backup_size,  
   msdb.dbo.backupmediafamily.logical_device_name,  
   msdb.dbo.backupmediafamily.physical_device_name,   
   msdb.dbo.backupset.name AS backupset_name, 
   msdb.dbo.backupset.description 
FROM   msdb.dbo.backupmediafamily  
   INNER JOIN msdb.dbo.backupset ON msdb.dbo.backupmediafamily.media_set_id = msdb.dbo.backupset.media_set_id 
WHERE  (CONVERT(datetime, msdb.dbo.backupset.backup_start_date, 102) >= GETDATE() - 7) 
--AND msdb.dbo.backupset.database_name='Protheus' 
ORDER BY  
   msdb.dbo.backupset.database_name, 
   msdb.dbo.backupset.backup_finish_date DESC;

-- LOG FILE USAGE

DBCC SQLPERF(logspace);

--BACKUP PROGRESS
SELECT      command, percent_complete,
            'elapsed' = total_elapsed_time / 60000.0,
            'remaining' = estimated_completion_time / 60000.0
FROM        sys.dm_exec_requests
WHERE       command like 'BACKUP%';

--GET PERFORMANCE INFO (PERF COUNTERS)

--CLEAR AGGREATED WAITS (limpar waits acumulados)

dbcc sqlperf ('sys.dm_os_wait_stats', clear);

--GET WAITS
SELECT  wait_type,waiting_tasks_count,wait_time_ms,max_wait_time_ms,
 signal_wait_time_ms,wait_time_ms/waiting_tasks_count as avg
 FROM sys.dm_os_wait_stats 
 WHERE waiting_tasks_count>0
 order by avg desc;

--GET WHO'S WAITING
SELECT sess.session_id,sess.host_name,sess.program_name,sess.login_name, waits.wait_duration_ms/1000 AS seconds_in_wait, waits.wait_type, waits.blocking_session_id 
FROM sys.dm_os_waiting_tasks waits inner join sys.dm_exec_sessions sess on (sess.session_id=waits.session_id);

--MORE WAITS
WITH [Waits] AS
    (SELECT
        [wait_type],
        [wait_time_ms] / 1000.0 AS [WaitS],
        ([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 AS [ResourceS],
        [signal_wait_time_ms] / 1000.0 AS [SignalS],
        [waiting_tasks_count] AS [WaitCount],
        100.0 * [wait_time_ms] / SUM ([wait_time_ms]) OVER() AS [Percentage],
        ROW_NUMBER() OVER(ORDER BY [wait_time_ms] DESC) AS [RowNum]
    FROM sys.dm_os_wait_stats
    WHERE [wait_type] NOT IN (
        N'BROKER_EVENTHANDLER',             N'BROKER_RECEIVE_WAITFOR',
        N'BROKER_TASK_STOP',                N'BROKER_TO_FLUSH',
        N'BROKER_TRANSMITTER',              N'CHECKPOINT_QUEUE',
        N'CHKPT',                           N'CLR_AUTO_EVENT',
        N'CLR_MANUAL_EVENT',                N'CLR_SEMAPHORE',
        N'DBMIRROR_DBM_EVENT',              N'DBMIRROR_EVENTS_QUEUE',
        N'DBMIRROR_WORKER_QUEUE',           N'DBMIRRORING_CMD',
        N'DIRTY_PAGE_POLL',                 N'DISPATCHER_QUEUE_SEMAPHORE',
        N'EXECSYNC',                        N'FSAGENT',
        N'FT_IFTS_SCHEDULER_IDLE_WAIT',     N'FT_IFTSHC_MUTEX',
        N'HADR_CLUSAPI_CALL',               N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
        N'HADR_LOGCAPTURE_WAIT',            N'HADR_NOTIFICATION_DEQUEUE',
        N'HADR_TIMER_TASK',                 N'HADR_WORK_QUEUE',
        N'KSOURCE_WAKEUP',                  N'LAZYWRITER_SLEEP',
        N'LOGMGR_QUEUE',                    N'ONDEMAND_TASK_QUEUE',
        N'PWAIT_ALL_COMPONENTS_INITIALIZED',
        N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
        N'REQUEST_FOR_DEADLOCK_SEARCH',     N'RESOURCE_QUEUE',
        N'SERVER_IDLE_CHECK',               N'SLEEP_BPOOL_FLUSH',
        N'SLEEP_DBSTARTUP',                 N'SLEEP_DCOMSTARTUP',
        N'SLEEP_MASTERDBREADY',             N'SLEEP_MASTERMDREADY',
        N'SLEEP_MASTERUPGRADED',            N'SLEEP_MSDBSTARTUP',
        N'SLEEP_SYSTEMTASK',                N'SLEEP_TASK',
        N'SLEEP_TEMPDBSTARTUP',             N'SNI_HTTP_ACCEPT',
        N'SP_SERVER_DIAGNOSTICS_SLEEP',     N'SQLTRACE_BUFFER_FLUSH',
        N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
        N'SQLTRACE_WAIT_ENTRIES',           N'WAIT_FOR_RESULTS',
        N'WAITFOR',                         N'WAITFOR_TASKSHUTDOWN',
        N'WAIT_XTP_HOST_WAIT',              N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG',
        N'WAIT_XTP_CKPT_CLOSE',             N'XE_DISPATCHER_JOIN',
        N'XE_DISPATCHER_WAIT',              N'XE_TIMER_EVENT')
    AND [waiting_tasks_count] > 0
 )
SELECT
    MAX ([W1].[wait_type]) AS [WaitType],
    CAST (MAX ([W1].[WaitS]) AS DECIMAL (16,2)) AS [Wait_S],
    CAST (MAX ([W1].[ResourceS]) AS DECIMAL (16,2)) AS [Resource_S],
    CAST (MAX ([W1].[SignalS]) AS DECIMAL (16,2)) AS [Signal_S],
    MAX ([W1].[WaitCount]) AS [WaitCount],
    CAST (MAX ([W1].[Percentage]) AS DECIMAL (5,2)) AS [Percentage],
    CAST ((MAX ([W1].[WaitS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgWait_S],
    CAST ((MAX ([W1].[ResourceS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgRes_S],
    CAST ((MAX ([W1].[SignalS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgSig_S]
FROM [Waits] AS [W1]
INNER JOIN [Waits] AS [W2]
    ON [W2].[RowNum] <= [W1].[RowNum]
GROUP BY [W1].[RowNum]
HAVING SUM ([W2].[Percentage]) - MAX ([W1].[Percentage]) < 95; -- percentage threshold
GO

--PAGE LIFE EXPECTANCY (good > 300)
SELECT object_name, counter_name, cntr_value
FROM sys.dm_os_performance_counters
WHERE [object_name] LIKE '%Buffer Manager%'
AND [counter_name] = 'Page life expectancy'

--BUFFER CACHE HIT RATIO (good > 90)
SELECT object_name, counter_name, cntr_value
FROM sys.dm_os_performance_counters
WHERE [object_name] LIKE '%Buffer Manager%'
AND [counter_name] = 'Buffer cache hit ratio'

--

--TEMPDB USAGE
SELECT SUM(unallocated_extent_page_count) AS [free pages], 
(SUM(unallocated_extent_page_count)*1.0/128) AS [free space in MB]
FROM sys.dm_db_file_space_usage;

--RETORNA O CONTEUDO DE UM TRACE EM TABELA
SELECT * FROM fn_trace_gettable ( 'D:\LB2\SSQLProfiles\Producao_manha_20150827_queries_4s.trc' , 1 )
ORDER BY EndTime
------------------------------

-- PRIMARY KEY COLUMNS

SELECT DB_NAME() AS Database_Name,sc.name AS 'Schema_Name',o.Name AS 'Table_Name',i.Name AS 'Index_Name',c.Name AS 'Column_Name',ic.key_ordinal,i.type_desc AS 'Index_Type'
FROM sys.indexes i
INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
INNER JOIN sys.columns c ON ic.object_id = c.object_id and ic.column_id = c.column_id
INNER JOIN sys.objects o ON i.object_id = o.object_id
INNER JOIN sys.schemas sc ON o.schema_id = sc.schema_id
WHERE i.is_primary_key = 1
AND sc.name = 'Schema_Name'
AND o.name = 'Table_Name'
ORDER BY o.Name, i.Name, ic.key_ordinal

######################### CHEAT SHEET #########################

--To create a SQL Server login that uses Windows Authentication using Transact-SQL
CREATE LOGIN <name of Windows User> FROM WINDOWS; GO     
--To create a SQL Server login that uses SQL Server Authentication (Transact-SQL)
CREATE LOGIN <login name> WITH PASSWORD = '<password>' ; GO   

--To create a database user using Transact-SQL
--Connect to the database in which to create the new database user:
USE <database name> GO  
CREATE USER <new user name> FOR LOGIN <login name> ; GO 

--Disable all users to see all databases
--The Owner cannot be an existing DATABASE user (note: database user != login != schema)
USE MASTER
GO
DENY VIEW ANY DATABASE TO PUBLIC
GO
USE SISTEMA
GO
SP_changedbowner SISTEMA
GO

--Create table as select
USE SISTEMA
GO
SELECT *
INTO Createdbysistema
FROM SISTEMA_RESTORED..Createdbysistema
GO

--Insert from select
insert into Createdbysistema select * from SISTEMA_RESTORED..Createdbysistema

--DROP DATABASE
USE [master]
GO
EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'SISTEMA_RESTORED'
GO
ALTER DATABASE [SISTEMA_RESTORED] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE
GO
DROP DATABASE [SISTEMA_RESTORED]
GO

------------------------------

######################### COOL JOBS #########################

EXEC msdb.dbo.sp_send_dbmail
    @profile_name = 'SQLServerEmail',
    @recipients = 'tiago.furlaneto@lb2.com.br',
    @query = 'select @@servername;EXEC master..xp_fixeddrives;select left(physical_name,2) as LogLocation, (size*8)/1024 as size_in_mb from MyDatabase.sys.database_files where type_desc=''LOG'';' ,
    @subject = 'Acompanhamento de tamanho dos logs';
    --@attach_query_result_as_file = 1 ;

------------------------------

######################### BACKUP COMMANDS #########################

--You must back up the master database. The master database records all of the system-level information for a Microsoft SQL Server system, such as login accounts, system configuration settings, endpoints and credentials, and the information required to access the other databases. The master database also records the initialization information that is required for a server instance to start. For more information, see master Database.

--O banco de dados msdb é usado pelo SQL Server, SQL Server Management Studio e pelo SQL Server Agent para armazenar dados, incluindo informações de agendamento e informações de histórico de restauração e backup.

----------------------------------
-------- DATABASE BACKUPS --------
----------------------------------

BACKUP DATABASE MyDatabase TO DISK='D:\Backups\MyDatabase_Full.BAK'
with COPY_ONLY, INIT, name = 'MyDatabase_Bkp_Full'
GO

#COPY_ONLY
#Specifies that the backup is a copy-only backup, which does not affect the normal sequence of backups. A copy-only backup is created independently of your regularly scheduled, conventional backups. A copy-only backup does not affect your overall backup and restore procedures for the database.

#Specifies that all backup sets should be overwritten, but preserves the media header. If INIT is specified, any existing backup set on that device is overwritten, if conditions permit. By default, BACKUP checks for the following conditions and does not overwrite the backup media if either condition exists:
# - Any backup set has not yet expired. For more information, see the EXPIREDATE and RETAINDAYS options.
# - The backup set name given in the BACKUP statement, if provided, does not match the name on the backup media. For more information, see the NAME option, earlier in this section.

-----------------------------
-------- LOG BACKUPS --------
-----------------------------

BACKUP LOG [SISTEMA] TO DISK = N'D:\BACKUP\sistema.trn' WITH INIT; 
GO
#INIT WILL OVERWRITE THE BACKUP, PREVIOUS LOGS BACKUPS INSIDE IT WILL BE ERASED

--OR

BACKUP LOG [SISTEMA] TO DISK = N'D:\BACKUP\sistema.trn' WITH INIT, NO_TRUNCATE;
GO

--IF TRUNCATE WAS USED:

DBCC SHRINKFILE (SISTEMA_Log)
GO

#For a database that uses either the full or bulk-logged recovery model, you generally need to back up the tail of the log before beginning to restore the database. You also should back up the tail of the log of the primary database before failing over a log shipping configuration. Restoring the tail-log backup as the final log backup before recovering the database avoids work loss after a failure.

######################### RESTORE COMMANDS #########################

RESTORE DATABASE [SISTEMA] FROM  DISK = N'D:\BACKUP\SISTEMA.bak' WITH  FILE = 1,  
STANDBY = N'D:\Backup\ROLLBACK_UNDO_SISTEMA.BAK',  
NOUNLOAD,  REPLACE,  STATS = 10
GO

# Restaura a base, e a deixa em standby / read only, permitindo aplicar mais transaction logs

--------
RESTORE LOG [SISTEMA] FROM  DISK = N'D:\BACKUP\sistema.trn' WITH  FILE = 1,  
NOUNLOAD,  STATS = 10
GO
--------
restore database SISTEMA with recovery;

#Se uma base está no estado "restoring" ou "Stand by / Read Only" isto irá cancelar o recovery e abrir a base.

--------
--"SQLServerAgent Error: Request to run job MaintenancePlan.Subplan_1 (from User DBA) refused because the job has been suspended."
-- https://support.microsoft.com/en-us/kb/914171
# IF YOU DID RESTORED THE MSDB DATABASE IN A DIFFERENT LOCATION, YOUR JOBS WILL PROBABLY FAIL, YOU NEED TO EXECUTE:
use msdb
go
delete from msdb.dbo.syssubsystems
exec msdb.dbo.sp_verify_subsystems 1
go

select * from msdb.dbo.syssubsystems;

--------

RESTORE DATABASE [SISTEMA_RESTORED] 
    FROM  DISK = N'E:\Backup\SISTEMA_backup_2016_03_31_145716_8803186.bak' WITH  FILE = 1,  
        MOVE N'SISTEMA' TO N'E:\Microsoft SQL Server\MSSQL10_50.SQL2008\MSSQL\DATA\SISTEMA_RESTORED.mdf',  
        MOVE N'SISTEMA_log' TO N'E:\Microsoft SQL Server\MSSQL10_50.SQL2008\MSSQL\DATA\SISTEMA_RESTORED_1.ldf',  
    NOUNLOAD,  STATS = 10
GO

# Restore database to a different database and place

--------
RESTORE LOG [SISTEMA] FROM  DISK = N'E:\BACKUP\sistema.trn' WITH  FILE = 1,  NORECOVERY,  NOUNLOAD,  STATS = 10
GO
RESTORE LOG [SISTEMA] FROM  DISK = N'E:\BACKUP\sistema.trn' WITH  FILE = 2,  
STANDBY = N'E:\Microsoft SQL Server\Backup\ROLLBACK_UNDO_SISTEMA.BAK',  NOUNLOAD,  STATS = 10
GO

# Restore log backups that are inside one file ony



