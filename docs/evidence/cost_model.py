"""AirBreda cost model, eu-north-1, on-demand, 730 h/month. All unit prices from the AWS Price List API.

Public price pages to check them (select Region "Europe (Stockholm)"):
  https://aws.amazon.com/ec2/pricing/on-demand/        https://aws.amazon.com/ebs/pricing/
  https://aws.amazon.com/rds/postgresql/pricing/       https://aws.amazon.com/s3/pricing/
  https://aws.amazon.com/vpc/pricing/ (public IPv4)    https://aws.amazon.com/fargate/pricing/
  https://aws.amazon.com/elasticloadbalancing/pricing/ https://aws.amazon.com/ecr/pricing/
"""
H = 730
DAYS_MONTH = H / 24          # 30.4167
EUR_PER_USD = 1 / 1.1355     # ECB reference rate 2026-09-30: 1 EUR = 1.1355 USD

P = {
    "t3.micro": 0.0108, "t3.small": 0.0216, "t3.medium": 0.0432,
    "ebs_gp3": 0.0836, "ipv4": 0.005,
    "db.t4g.micro": 0.016, "db.t4g.small": 0.033, "db.t4g.medium": 0.065,
    "rds_gp3": 0.12, "rds_backup": 0.09,
    "s3_std": 0.023, "s3_put": 0.005 / 1000, "s3_get": 0.0004 / 1000,
    # eu-west-1 (Ireland) for DR
    "ie_t3.micro": 0.0114, "ie_ebs_gp3": 0.088, "ie_ebs_snap": 0.05,
    "ie_db.t4g.micro": 0.017, "ie_rds_gp3": 0.127, "ie_rds_backup": 0.095,
    "ie_s3_std": 0.023, "ie_s3_put": 0.005 / 1000, "ie_ipv4": 0.005,
    "xfer_eun1_to_eu": 0.02, "r53_zone": 0.50,
}

RAW_MB = 1.18      # measured: raw/ndw/2026-10-01/*.xml.gz, 1,179,747..1,181,084 bytes
CONFIG_MB = 1.84   # measured: raw/ndw/config/2026-10-01.xml.gz, 1,842,617 bytes
CSV_KB_10MIN = 1.0 # measured: ndw/2026-10-01/09-*.csv, 944..1,050 bytes with 6 rows
ROW_BYTES = 200    # PostgreSQL heap tuple ~100 B + PK index entry ~80 B + overhead


def scenario(name, corridors, interval_min, vm, db, db_gb, ebs_gb=16):
    sites = 4 * corridors
    runs = H * 60 / interval_min                    # traffic runs per month
    samples_h = 60 / interval_min
    # compute
    vm_usd = P[vm] * H
    ebs_usd = P["ebs_gp3"] * ebs_gb
    vm_ip = P["ipv4"] * H
    compute = vm_usd + ebs_usd + vm_ip
    # database
    db_usd = P[db] * H
    db_stor = P["rds_gp3"] * db_gb
    db_ip = P["ipv4"] * H
    rows_year = corridors * (8760 + 4 * 2 * samples_h * 8760)
    db_gb_year = rows_year * ROW_BYTES / 1e9
    backup = 0.0  # 1-day retention, backup < provisioned storage => free allocation covers it
    database = db_usd + db_stor + db_ip + backup
    # object storage, month 12 (12 months accumulated, no lifecycle)
    raw_mb_day = (24 * samples_h) * RAW_MB + CONFIG_MB
    raw_gb = raw_mb_day * 365 / 1000
    csv_gb = sites * 8760 * CSV_KB_10MIN * (samples_h / 6) / 1e6
    stored_gb = raw_gb + csv_gb
    s3_stor = stored_gb * P["s3_std"]
    # requests per month (ingest_traffic.py + common.S3Store + dashboard.py)
    put_ingest = runs * (1 + sites) + round(DAYS_MONTH)   # raw PUT + CSV PUT per site + daily config PUT
    get_ingest = runs * (1 + sites)                          # config HEAD + CSV GET per site
    rounds = H * 60                                          # dashboard: one S3 round per 60 s cache window (upper bound)
    keys_day = sites * 24
    list_pages = -(-keys_day // 1000)
    put_dash = rounds * list_pages                           # LIST billed at PUT tier
    get_dash = rounds * sites
    puts = put_ingest + put_dash
    gets = get_ingest + get_dash
    s3_req = puts * P["s3_put"] + gets * P["s3_get"]
    objstore = s3_stor + s3_req
    total = compute + database + objstore
    return dict(name=name, corridors=corridors, vm=vm, db=db, db_gb=db_gb, ebs_gb=ebs_gb,
                vm_usd=vm_usd, ebs_usd=ebs_usd, vm_ip=vm_ip, compute=compute,
                db_usd=db_usd, db_stor=db_stor, db_ip=db_ip, database=database,
                rows_year=rows_year, db_gb_year=db_gb_year,
                raw_mb_day=raw_mb_day, raw_gb=raw_gb, csv_gb=csv_gb, stored_gb=stored_gb,
                s3_stor=s3_stor, puts=puts, gets=gets, put_ingest=put_ingest, get_ingest=get_ingest,
                put_dash=put_dash, get_dash=get_dash, s3_req=s3_req, objstore=objstore,
                total=total, runs=runs)


S = [
    scenario("Current (1 corridor)", 1, 10, "t3.micro", "db.t4g.micro", 20),
    scenario("10 corridors", 10, 10, "t3.small", "db.t4g.micro", 20),
    scenario("50 corridors", 50, 5, "t3.medium", "db.t4g.medium", 50),
]


def eur(x):
    return round(x * EUR_PER_USD, 2)


for s in S:
    print(f"\n== {s['name']} ==")
    for k, v in s.items():
        if isinstance(v, float):
            print(f"  {k:12s} {v:14.4f}" + (f"   EUR {eur(v):.2f}" if k in ("compute", "database", "objstore", "total") else ""))
        else:
            print(f"  {k:12s} {v}")
    lines = [eur(s["compute"]), eur(s["database"]), eur(s["objstore"])]
    print("  EUR lines", lines, "sum of rounded", round(sum(lines), 2), "rounded total", eur(s["total"]))

# --- DR tiers for the current system (second Region eu-west-1 Ireland) ---
cur = S[0]
print("\n== DR tiers ==")
base = cur["total"]
# Pilot light: cross-Region read replica + AMI + S3 CRR
rep_inst = P["ie_db.t4g.micro"] * H
rep_stor = P["ie_rds_gp3"] * 20
rep_xfer = 5 * P["xfer_eun1_to_eu"]            # assume 5 GB WAL/month
ami = 6 * P["ie_ebs_snap"]                     # 6 GB of used blocks in the AMI snapshot
crr_stor = cur["stored_gb"] * P["ie_s3_std"]   # month 12 replica copy
crr_put = cur["put_ingest"] * P["ie_s3_put"]   # every replicated object write is a PUT in the destination
crr_xfer = (cur["raw_gb"] + cur["csv_gb"]) / 12 * P["xfer_eun1_to_eu"]
crr = crr_stor + crr_put + crr_xfer
pilot = rep_inst + rep_stor + rep_xfer + ami + crr
# cheaper variant: daily RDS snapshot copy instead of replica
snap_copy = 2 * P["ie_rds_backup"] + 2 * P["xfer_eun1_to_eu"]
pilot_snap = snap_copy + ami + crr
# Warm standby: replica + running t3.micro + EBS + IPv4 + S3 CRR + Route 53 zone (health check free, first 50)
ws_vm = P["ie_t3.micro"] * H
ws_ebs = P["ie_ebs_gp3"] * 16
ws_ip = P["ie_ipv4"] * H
warm = rep_inst + rep_stor + rep_xfer + ws_vm + ws_ebs + ws_ip + crr + P["r53_zone"]
for label, v in [("rep_inst", rep_inst), ("rep_stor", rep_stor), ("rep_xfer", rep_xfer), ("ami", ami),
                 ("crr_stor", crr_stor), ("crr_put", crr_put), ("crr_xfer", crr_xfer), ("crr", crr),
                 ("ws_vm", ws_vm), ("ws_ebs", ws_ebs), ("ws_ip", ws_ip), ("snap_copy", snap_copy)]:
    print(f"  {label:10s} USD {v:8.4f}  EUR {eur(v):.2f}")
for label, extra in [("Backup and Restore (current)", 0.0), ("Pilot Light (snapshot copy variant)", pilot_snap),
                     ("Pilot Light (replica)", pilot), ("Warm Standby", warm)]:
    print(f"  {label:38s} extra USD {extra:7.4f} EUR {eur(extra):6.2f} | total USD {base+extra:7.4f} EUR {eur(base+extra):6.2f}")
# --- In-Region Multi-AZ RDS (ADR-003, section 3 DR): covers an AZ failure, not a Region ---
P.update({"db.t4g.micro_maz": 0.033, "rds_gp3_maz": 0.24})   # Price List API, Multi-AZ, eu-north-1
maz_extra = (P["db.t4g.micro_maz"] - P["db.t4g.micro"]) * H + (P["rds_gp3_maz"] - P["rds_gp3"]) * 20
print(f"\n== Multi-AZ RDS uplift == extra USD {maz_extra:.4f} EUR {eur(maz_extra):.2f}"
      f" | total EUR {eur(base + maz_extra):.2f}")

# --- Rejected compute alternative (ADR-004): ECS Fargate + ALB + EventBridge Scheduler ---
# Price List API, eu-north-1, x86: Fargate vCPU-hour 0.0445, GB-hour 0.0049; ALB hour 0.02394,
# LCU-hour 0.0076; ECR storage 0.10 per GB-month. EventBridge Scheduler: 14M invocations/month free.
P.update({"fg_vcpu": 0.0445, "fg_gb": 0.0049, "alb_h": 0.02394, "alb_lcu": 0.0076, "ecr_gb": 0.10})
task_h = 0.25 * P["fg_vcpu"] + 0.5 * P["fg_gb"]       # smallest task size: 0.25 vCPU, 0.5 GB
fg_dash = task_h * H                                   # dashboard task, always on
fg_dash_ip = P["ipv4"] * H                             # public subnet without NAT gateway: task needs a public IPv4
fg_alb = P["alb_h"] * H + 0.1 * P["alb_lcu"] * H       # ALB hour + about 0.1 LCU on average (a few requests a minute)
fg_alb_ip = 2 * P["ipv4"] * H                          # internet-facing ALB spans 2 AZs, one public IPv4 each
job_runs = H * 60 / 10 + H                             # traffic every 10 min + air hourly = 5,110 runs
job_h = job_runs * 60 / 3600                           # Fargate bills at least 1 minute per task; runs take about 4 s
fg_jobs = job_h * (task_h + P["ipv4"])
fg_ecr = 1.0 * P["ecr_gb"]                             # three images, about 1 GB compressed
fargate = fg_dash + fg_dash_ip + fg_alb + fg_alb_ip + fg_jobs + fg_ecr
fargate_no_alb = fargate - fg_alb - fg_alb_ip
print("\n== Fargate alternative (current load) ==")
for label, v in [("dashboard task", fg_dash), ("dashboard IPv4", fg_dash_ip), ("ALB", fg_alb),
                 ("ALB IPv4 x2", fg_alb_ip), ("job tasks", fg_jobs), ("ECR", fg_ecr),
                 ("TOTAL Fargate + ALB", fargate), ("TOTAL Fargate, no ALB", fargate_no_alb),
                 ("VM (current compute)", cur["compute"])]:
    print(f"  {label:24s} USD {v:8.4f}  EUR {eur(v):6.2f}")
print(f"  job runs/month {job_runs:.0f}, billed task-hours {job_h:.2f}")

print("\nEUR_PER_USD", EUR_PER_USD)
print("credits months at current list price:", 100 / cur["total"])
print("current per day USD", cur["total"] / DAYS_MONTH, "month-1 storage est per day",
      (cur["compute"] + cur["database"] + cur["raw_gb"] / 12 * 0.5 * P["s3_std"] + cur["s3_req"]) / DAYS_MONTH)
