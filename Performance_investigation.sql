-- Requires these rights.
grant view server state to [login name];
go

-- Top 10 statements by write I/O.
-- Because the lazy writer asynchronously does physical write operations, only logical writes can be associated with a statement.
select top(10) sum(Q.total_logical_writes) as [Logical writes],
               sum(Q.execution_count) as [Execution count],
               min(coalesce(quotename(db_name(Q.dbid)), N'-') + N'.' + coalesce(quotename(object_schema_name(Q.objectid, Q.dbid)), N'-') + N'.' + coalesce(quotename(object_name(Q.objectid, Q.dbid)), N'-')) as [Module],
               min(Q.statement_text) as [Statement]
from (select QS.*,
             ST.dbid as dbid,
             ST.objectid as objectid,
             substring(ST.text,
                      (QS.statement_start_offset/2) + 1,
                      ((case statement_end_offset 
                        when -1 then datalength(ST.text)
                        else QS.statement_end_offset end 
                        - QS.statement_start_offset)/2) + 1) as statement_text
      from sys.dm_exec_query_stats as QS
           cross apply sys.dm_exec_sql_text(QS.sql_handle) as ST) as Q
-- where Q.dbid = db_id()
group by Q.query_hash
order by [Logical Writes] desc;
go

-- Top 10 statements by physical read I/O.
select top(10) sum(Q.total_physical_reads) as [Physical reads],
               sum(Q.total_logical_reads) as [Logical reads],
               sum(Q.execution_count) as [Execution count],
               min(coalesce(quotename(db_name(Q.dbid)), N'-') + N'.' + coalesce(quotename(object_schema_name(Q.objectid, Q.dbid)), N'-') + N'.' + coalesce(quotename(object_name(Q.objectid, Q.dbid)), N'-')) as [Module],
               min(Q.statement_text) as [Statement]
from (select QS.*,
             ST.dbid as dbid,
             ST.objectid as objectid,
             substring(ST.text,
                      (QS.statement_start_offset/2) + 1,
                      ((case statement_end_offset 
                        when -1 then datalength(ST.text)
                        else QS.statement_end_offset end 
                        - QS.statement_start_offset)/2) + 1) as statement_text
      from sys.dm_exec_query_stats as QS
           cross apply sys.dm_exec_sql_text(QS.sql_handle) as ST) as Q
where Q.dbid = db_id('MP10DZ')
group by Q.query_hash
order by [Physical Reads] desc;
go

-- Top 10 statements by logical read I/O.
select top(10) sum(Q.total_physical_reads) as [Physical reads],
               sum(Q.total_logical_reads) as [Logical reads],
               sum(Q.execution_count) as [Execution count],
               min(coalesce(quotename(db_name(Q.dbid)), N'-') + N'.' + coalesce(quotename(object_schema_name(Q.objectid, Q.dbid)), N'-') + N'.' + coalesce(quotename(object_name(Q.objectid, Q.dbid)), N'-')) as [Module],
               min(Q.statement_text) as [Statement]
from (select QS.*,
             ST.dbid as dbid,
             ST.objectid as objectid,
             substring(ST.text,
                      (QS.statement_start_offset/2) + 1,
                      ((case statement_end_offset 
                        when -1 then datalength(ST.text)
                        else QS.statement_end_offset end 
                        - QS.statement_start_offset)/2) + 1) as statement_text
      from sys.dm_exec_query_stats as QS
           cross apply sys.dm_exec_sql_text(QS.sql_handle) as ST) as Q
--where Q.dbid = db_id()
group by Q.query_hash
order by [Logical Reads] desc;
go

-- Top 10 statements by CPU time.
select top(10) sum(convert(decimal(38, 6), Q.total_worker_time)/1000000) as [CPU (secs)],
               sum(Q.execution_count) as [Execution count],
               min(coalesce(quotename(db_name(Q.dbid)), N'-') + N'.' + coalesce(quotename(object_schema_name(Q.objectid, Q.dbid)), N'-') + N'.' + coalesce(quotename(object_name(Q.objectid, Q.dbid)), N'-')) as [Module],
               min(Q.statement_text) as [Statement]
from (select QS.*,
             ST.dbid as dbid,
             ST.objectid as objectid,
             substring(ST.text,
                      (QS.statement_start_offset/2) + 1,
                      ((case statement_end_offset 
                        when -1 then datalength(ST.text)
                        else QS.statement_end_offset end 
                        - QS.statement_start_offset)/2) + 1) as statement_text
      from sys.dm_exec_query_stats as QS
           cross apply sys.dm_exec_sql_text(QS.sql_handle) as ST) as Q
--where Q.dbid = db_id()
group by Q.query_hash
order by [CPU (secs)] desc;
go

-- Top 10 modules by write I/O.
-- Because the lazy writer asynchronously does physical write operations, only logical writes can be associated with a module.
select top(10) D.total_logical_writes as [Total logical writes],
               D.execution_count as [Executions],
               coalesce(quotename(db_name(D.database_id)), N'-') + N'.' + coalesce(quotename(object_schema_name(D.object_id, D.database_id)), N'-') + N'.' + coalesce(quotename(object_name(D.object_id, D.database_id)), N'-') as [Module],
               QP.query_plan as [Query plan]
from sys.dm_exec_procedure_stats as D
     outer apply sys.dm_exec_query_plan(D.plan_handle) as QP
--where D.database_id = db_id()
order by [Total logical writes] desc;
go

-- Top 10 modules by physical read I/O.
select top(10) D.total_physical_reads as [Total physical reads],
               D.total_logical_reads as [Total logical reads],
               D.execution_count as [Executions],
               coalesce(quotename(db_name(D.database_id)), N'-') + N'.' + coalesce(quotename(object_schema_name(D.object_id, D.database_id)), N'-') + N'.' + coalesce(quotename(object_name(D.object_id, D.database_id)), N'-') as [Module],
               QP.query_plan as [Query plan]
from sys.dm_exec_procedure_stats as D
     outer apply sys.dm_exec_query_plan(D.plan_handle) as QP
--where D.database_id = db_id()
order by [Total physical reads] desc;
go

-- Top 10 modules by logical read I/O.
select top(10) D.total_physical_reads as [Total physical reads],
               D.total_logical_reads as [Total logical reads],
               D.execution_count as [Executions],
               coalesce(quotename(db_name(D.database_id)), N'-') + N'.' + coalesce(quotename(object_schema_name(D.object_id, D.database_id)), N'-') + N'.' + coalesce(quotename(object_name(D.object_id, D.database_id)), N'-') as [Module],
               QP.query_plan as [Query plan]
from sys.dm_exec_procedure_stats as D
     outer apply sys.dm_exec_query_plan(D.plan_handle) as QP
--where D.database_id = db_id()
order by [Total logical reads] desc;
go

-- Top 10 modules by CPU time.
select top(10) convert(decimal(38, 6), D.total_worker_time)/1000000 as [CPU (secs)],
               D.execution_count as [Executions],
               coalesce(quotename(db_name(D.database_id)), N'-') + N'.' + coalesce(quotename(object_schema_name(D.object_id, D.database_id)), N'-') + N'.' + coalesce(quotename(object_name(D.object_id, D.database_id)), N'-') as [Module],
               QP.query_plan as [Query plan]
from sys.dm_exec_procedure_stats as D
     outer apply sys.dm_exec_query_plan(D.plan_handle) as QP
--where D.database_id = db_id()
order by D.total_worker_time desc;
go

-- Total I/O
select sum(VFS.num_of_reads) as [Reads (operations)],
       convert(decimal(38,2), sum(VFS.num_of_bytes_read/1048576.0)) as [Data read (MB)],
       sum(VFS.num_of_writes) as [Writes (operations)],
       convert(decimal(38,2), sum(VFS.num_of_bytes_written/1048576.0)) as [Data written (MB)]
from sys.dm_io_virtual_file_stats(null, null) as VFS;
go

-- Top 10 databases by physical write I/O.
select top(10) quotename(db_name(VFS.database_id)) as [Database],
               convert(decimal(38,2), sum(VFS.num_of_bytes_written/1048576.0)) as [Data written (MB)],
               sum(VFS.num_of_writes) as [Total writes],
               convert(decimal(38,2), sum(VFS.size_on_disk_bytes/1048576.0)) as [Size (MB)]
from sys.dm_io_virtual_file_stats(null, null) as VFS
group by VFS.database_id
order by [Data written (MB)] desc;
go

-- Top 10 databases by physical read I/O.
select top(10) quotename(db_name(VFS.database_id)) as [Database],
               convert(decimal(38,2), sum(VFS.num_of_bytes_read/1048576.0)) as [Data read (MB)],
               sum(VFS.num_of_reads) as [Total reads],
               convert(decimal(38,2), sum(VFS.size_on_disk_bytes/1048576.0)) as [Size (MB)]
from sys.dm_io_virtual_file_stats(null, null) as VFS
group by VFS.database_id
order by [Data read (MB)] desc;
go

-- Top 10 recompiles.
select top(10) max(coalesce(quotename(db_name(SQ.dbid)), N'-') + N'.' + coalesce(quotename(object_schema_name(SQ.objectid, SQ.dbid)), N'-') + N'.' + coalesce(quotename(object_name(SQ.objectid, SQ.dbid)), N'-')) as [Module],
               max(QS.plan_generation_num) as [Recompiles], 
               sum(QS.execution_count) as [Executions],
               SQ.text as [Text]               
from sys.dm_exec_query_stats as QS
     cross apply sys.dm_exec_sql_text(QS.sql_handle) as SQ
--where SQ.dbid = db_id()
group by SQ.text
order by [Recompiles] desc, [Executions], [Module];
go

-- Index fragmentation
select coalesce(quotename(D.name), N'-') + N'.' + coalesce(quotename(object_schema_name(IPS.object_id, D.database_id)), N'-') + N'.' + coalesce(quotename(object_name(IPS.object_id, D.database_id)), N'-') as [Object],
       I.name as [Index],
       IPS.partition_number as [Partition],
       IPS.index_level as [Level],
       convert(decimal(38,2), IPS.page_count/128.0) as [Size (MB)],
       IPS.avg_fragmentation_in_percent as [Mean fragementation (%)], -- Only relevant where in-order pages are faster to read or write.
       IPS.avg_page_space_used_in_percent as [Mean  space Used (%)]
from sys.databases as D
     cross apply sys.dm_db_index_physical_stats(D.database_id, null, null, null, N'SAMPLED' /* For non-leaf pages use N'DETAILED'*/) as IPS
     inner join sys.indexes as I on I.object_id = IPS.object_id and I.index_id = IPS.index_id
where D.database_id = db_id()
order by [Object], [Index], [Partition], [Level];
go

-- Fragmented indexes
select coalesce(quotename(D.name), N'-') + N'.' + coalesce(quotename(object_schema_name(IPS.object_id, D.database_id)), N'-') + N'.' + coalesce(quotename(object_name(IPS.object_id, D.database_id)), N'-') as [Object],
       I.name as [Index],
       IPS.partition_number as [Partition],
       IPS.index_level as [Level],
       convert(decimal(38,2), IPS.page_count/128.0) as [Size (MB)],
       IPS.avg_fragmentation_in_percent as [Mean fragementation (%)],
       IPS.avg_page_space_used_in_percent as [Mean  space Used (%)],
       case
         when IPS.avg_fragmentation_in_percent < 30.0 then N'Reorganize'
         else N'Rebuild'
       end as [Action]
from sys.databases as D
     cross apply sys.dm_db_index_physical_stats(D.database_id, null, null, null, N'SAMPLED' /* For non-leaf pages use N'DETAILED'*/) as IPS
     inner join sys.indexes as I on I.object_id = IPS.object_id and
                                    I.index_id = IPS.index_id
where D.database_id = db_id() and
      IPS.alloc_unit_type_desc = 'IN_ROW_DATA' and
      ((IPS.page_count > 24 and IPS.avg_fragmentation_in_percent > 5.0) or             -- Fragmented (only relevant where in-order pages are faster to read or write).
      (IPS.page_count > 8 and IPS.avg_page_space_used_in_percent < I.fill_factor*0.9)) -- Compaction needed.
order by [Object], [Index], [Partition], [Level];
go
 
-- Index usage.
select coalesce(quotename(db_name(US.database_id)), N'-') + N'.' + coalesce(quotename(object_schema_name(US.object_id, US.database_id)), N'-') + N'.' + coalesce(quotename(object_name(US.object_id, US.database_id)), N'-') as [Object],
       I.name as [Index],
       convert(decimal(38,2), PS.[Page count]/128.0) as [Size (MB)],
       (US.user_seeks + US.user_scans + US.user_lookups) as [User operations],
       US.user_seeks as [User seeks],
       US.user_scans as [User scans],
       US.user_lookups as [User bookmark lookups],
       US.user_updates as [User updates]
from sys.dm_db_index_usage_stats as US
     cross apply (select IPS.object_id, IPS.index_id, sum(IPS.page_count)
                  from sys.dm_db_index_physical_stats(US.database_id, US.object_id, US.index_id, null, N'SAMPLED' /* For non-leaf pages use N'DETAILED'*/) as IPS
                  group by IPS.object_id, IPS.index_id) as PS ([Object], [Index], [Page count])
     inner join sys.indexes as I on I.object_id = US.object_id and
                                    I.index_id = US.index_id
where US.database_id = db_id()
order by [User Operations], [Object], [Index];
go

-- Missing indexes
select coalesce(quotename(db_name(MID.database_id)), N'-') + N'.' + coalesce(quotename(object_schema_name(MID.object_id, MID.database_id)), N'-') + N'.' + coalesce(quotename(object_name(MID.object_id, MID.database_id)), N'-') as [Object],
       round(MIGS.avg_total_user_cost * (MIGS.avg_user_impact/100.0) * (MIGS.user_seeks + MIGS.user_scans), 0) as [Relative cost],
       round(MIGS.avg_total_user_cost, 2) [Average cost],
       round(MIGS.avg_user_impact, 2) [Percentage improvement],
       MIGS.user_scans as [User scans],
       MIGS.user_seeks as [User seeks],
       coalesce(MID.equality_columns, N'') as [Equi-join],
       coalesce(MID.inequality_columns, N'') as [Inequi-join],
       coalesce(MID.included_columns, N'') as [Included]
from sys.dm_db_missing_index_group_stats as MIGS
     inner join sys.dm_db_missing_index_groups as MIG on MIG.index_group_handle = MIGS.group_handle
     inner join sys.dm_db_missing_index_details as MID on MID.index_handle = MIG.index_handle
where MID.database_id = db_id()
order by [Percentage Improvement] desc;
go

-- Object size
-- Fast approximate size.
select top(10) db_name() + N'.' + coalesce(quotename(object_schema_name(PS.object_id)), N'-') + N'.' + coalesce(quotename(object_name(PS.object_id)), N'-') as [Object],
               convert(decimal(38,2), sum(PS.used_page_count)/128.0) as [Size (MB)],
               sum(case when PS.index_id in (0, 1) then PS.row_count else 0 end) as [Rows],
               (sum(PS.used_page_count)*8000)/sum(case when PS.index_id in (0, 1) then PS.row_count else 0 end) as [Mean row size (Bytes)]

from sys.dm_db_partition_stats as PS
group by db_name() + N'.' + coalesce(quotename(object_schema_name(PS.object_id)), N'-') + N'.' + coalesce(quotename(object_name(PS.object_id)), N'-')
order by [Size (MB)] desc
go

-- Slow exact size.
select top (10) coalesce(quotename(db_name(D.database_id)), N'-') + N'.' + coalesce(quotename(object_schema_name(PS.object_id, D.database_id)), N'-') + N'.' + coalesce(quotename(object_name(PS.object_id, D.database_id)), N'-') as [Object],
                convert(decimal(38,2), sum(PS.page_count)/128.0) as [Size (MB)]
from sys.databases as D
     cross apply sys.dm_db_index_physical_stats(D.database_id, null, null, null, N'DETAILED') as PS
--where D.database_id = db_id()
group by coalesce(quotename(db_name(D.database_id)), N'-') + N'.' + coalesce(quotename(object_schema_name(PS.object_id, D.database_id)), N'-') + N'.' + coalesce(quotename(object_name(PS.object_id, D.database_id)), N'-')              
order by [Size (MB)] desc
go

-- Waits
-- See http://msdn.microsoft.com/en-us/library/ms179984.aspx for a description of types.

-- Before a test run, reset caches.
checkpoint;
dbcc freeproccache with no_infomsgs;
dbcc dropcleanbuffers with no_infomsgs;
dbcc sqlperf ('sys.dm_os_wait_stats', clear);
dbcc sqlperf ('sys.dm_os_latch_stats', clear);
go

-- After test run
select N'Lock total: ' + WS.wait_type as [Type],
       WS.waiting_tasks_count as [Waits (number)],
       convert(decimal(38,2), WS.wait_time_ms/1000.0) as [Total wait time (secs)],
       WS.wait_time_ms/WS.waiting_tasks_count as [Average wait time (ms)]
from sys.dm_os_wait_stats as WS
where WS.waiting_tasks_count > 0
union all
select N'Lock post signal: ' + WS.wait_type,
       WS.waiting_tasks_count,
       convert(decimal(38,2), WS.signal_wait_time_ms/1000.0),
       WS.signal_wait_time_ms/WS.waiting_tasks_count
from sys.dm_os_wait_stats as WS
where WS.waiting_tasks_count > 0
union all
select N'Latch total: ' + LS.latch_class,
       LS.waiting_requests_count,
       convert(decimal(38,2), LS.wait_time_ms/1000.0),
       LS.wait_time_ms/LS.waiting_requests_count
from sys.dm_os_latch_stats as LS
where LS.waiting_requests_count > 0
order by [Total wait time (secs)] desc;
go

-- Statements causing waits
-- See http://msdn.microsoft.com/en-us/library/ms179984.aspx for a description of types.
set nocount on;

while 1=1
begin
    declare @results table (
                               [Id]                       int identity not null primary key,
                               [Blocked module]           nvarchar(776) not null,
                               [Blocked statement]        nvarchar(max) not null,
                               [Blocking module]          nvarchar(776) null,
                               [Blocking statement]       nvarchar(max) null,
                               [Wait type]                nvarchar(60) not null,
                               [Total wait time (MS)]     bigint not null,
                               [Sessions waiting (count)] int not null
                           );
                           
    delete from @results;
    
    with Statements ([Blocked module], [Blocked statement start], [Blocked statement end],
                     [Blocking module], [Blocking statement start], [Blocking statement end],
                     [Wait type], [Total wait time (MS)], [Sessions waiting (count)])
    as
    (
      select BlockedR.sql_handle as [Blocked Module],
             BlockedR.statement_start_offset as [Blocked Statement start],
             BlockedR.statement_end_offset as [Blocked Statement end],
             BlockingR.sql_handle as [Blocking Module],
             BlockingR.statement_start_offset as [Blocking statement start],
             BlockingR.statement_end_offset as [Blocking statement end],
             WT.wait_type as [Wait type],
             sum(convert(bigint, WT.wait_duration_ms)) as [Total wait time (MS)],
             count(*) as [Sessions waiting (count)]
      from sys.dm_os_waiting_tasks as WT
           inner join sys.dm_exec_requests as BlockedR on BlockedR.session_id = WT.session_id
           left outer join sys.dm_exec_requests as BlockingR on BlockingR.session_id = WT.blocking_session_id
      group by BlockedR.sql_handle, BlockedR.statement_start_offset, BlockedR.statement_end_offset,
               BlockingR.sql_handle, BlockingR.statement_start_offset, BlockingR.statement_end_offset,
               WT.wait_type
    )
    insert into @results ([Blocked module], [Blocked statement],
                          [Blocking module], [Blocking statement],
                          [Wait type], [Total wait time (MS)], [Sessions waiting (count)])
    select coalesce(quotename(db_name(BlockedText.dbid)), N'-') + N'.' + coalesce(quotename(object_schema_name(BlockedText.objectid, BlockedText.dbid)), N'-') + N'.' + coalesce(quotename(object_name(BlockedText.objectid, BlockedText.dbid)), N'-') as [Blocked module],
           substring(BlockedText.text,
                     (S.[Blocked statement start]/2) + 1,
                     ((case S.[Blocked statement end] 
                         when -1 then datalength(BlockingText.text)
                         else S.[Blocked statement end]
                       end 
                       - S.[Blocked statement start])/2) + 1) as [Blocked statement],
           coalesce(quotename(db_name(BlockingText.dbid)), N'-') + N'.' + coalesce(quotename(object_schema_name(BlockingText.objectid, BlockingText.dbid)), N'-') + N'.' + coalesce(quotename(object_name(BlockingText.objectid, BlockingText.dbid)), N'-') as [Blocking module],
           substring(BlockingText.text,
                     (S.[Blocking statement start]/2) + 1,
                     ((case S.[Blocking statement end] 
                         when -1 then datalength(BlockingText.text)
                         else S.[Blocking statement end]
                       end 
                       - S.[Blocking statement start])/2) + 1) as [Blocking statement],
           S.[Wait type] as [Wait type],
           S.[Total wait time (MS)] as [Total wait time (MS)],
           S.[Sessions waiting (count)] as [Sessions waiting (count)]
    from Statements as S
         cross apply sys.dm_exec_sql_text(S.[Blocked module]) as BlockedText
         cross apply sys.dm_exec_sql_text(S.[Blocking module]) as BlockingText
    --where S.[Total wait time (MS)] > 50
    order by S.[Total wait time (MS)] desc
    option (force order);

  if exists (select * from @results)
  begin
    select getdate(),
           [Blocked module], [Blocked statement],
           [Blocking module], [Blocking statement],
           [Wait type], [Total wait time (MS)], [Sessions waiting (count)]
    from @results;
  end;

  waitfor delay '00:00:01'
end;


