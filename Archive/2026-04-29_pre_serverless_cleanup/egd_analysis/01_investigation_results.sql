/*
================================================================================
 EGD MACHINE INVESTIGATION - CIBatumi (192.168.77.15,17420)
 Database: CIBatumi  (SSMS shows as "CIBAnalytics")
 Date: 2026-02-25
================================================================================

 FINDINGS SUMMARY:
 =================

 1. SM_EgdCfg is NOT a dimension table - it's a VERSIONED CONFIG LOG
    - PK = (CasinoId, SiteId, MacAddr, SmibIp, Rev)
    - 4,930 rows but only 126 unique MacAddr (physical machines)
    - IpAddr is NULLABLE and NOT part of the PK
    - Each machine has 60-70 config revisions on average

 2. IpAddr = FLOOR POSITION (slot stand), NOT machine identity
    - Machines (MacAddr) rotate between floor positions (IpAddr)
    - A single IpAddr has hosted up to 126 different MacAddrs over time
    - A single MacAddr has been assigned up to 19 different IpAddrs

 3. SM_EgdCfg's InUse flag is MISLEADING
    - Shows 29 machines as InUse=1, ALL at IP 10.1.3.3 (parking/default)
    - Reality: 103 floor positions are active (have MeterDayV6 in last 30 days)

 4. COVERAGE GAPS:
    - 164 unique IpAddrs exist across all tables
    - SM_EgdCfg covers only 155 IPs
    - 9 IPs have fact data (sessions + meters) but ZERO config in SM_EgdCfg:
        10.1.3.6, 10.1.6.10, 10.1.14.3, 10.1.14.4, 10.1.14.6,
        10.1.14.8, 10.1.14.9, 10.1.14.10, 10.1.15.3

 5. Fact tables reference by IpAddr:
    - SM_PlayerSessionV7: 3,087,668 rows, 162 unique IPs (2015-10-16 to today)
    - SM_MeterDayV6:        360,077 rows, 162 unique IPs (2015-09-22 to today)

 RECOMMENDED APPROACH:
 =====================
 - Dimension keyed by IpAddr (floor position) since facts join on IpAddr
 - Enrich with latest machine info from SM_EgdCfg where available
 - Derive "active" status from actual fact table data, NOT from InUse flag
 - Separate bridge table for MacAddr<->IpAddr assignment history
*/
