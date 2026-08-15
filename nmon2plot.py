#!/usr/bin/env python3
# -- coding: utf-8 --

import os
import json
import glob
import re
from multiprocessing import Pool, cpu_count
import argparse

################################################################################
# 1. Helper Functions
################################################################################

def parse_date_time(date_str, time_str):
    """Combine date and time (e.g., '07-JAN-2025 00:01:54')."""
    return f"{date_str} {time_str}"

def read_in_chunks(file_object, chunk_size=10000):
    """Yield chunks (lists) of lines from the file."""
    chunk = []
    for line in file_object:
        chunk.append(line)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

################################################################################
# 2. parse_nmon_file (including TOP lines)
################################################################################

def parse_nmon_file(nmon_file):
    """
    Parses a .nmon file, extracting various statistics.
    (See the original comments for details.)
    """
    cpu_data_by_tag = {}
    lpar_data_by_tag = {}
    proc_data_by_tag = {}
    file_io_data_by_tag = {}
    top_data_by_tag = {}
    memnew_data_by_tag = {}
    mem_data_by_tag = {}
    mem_mb_data_by_tag = {}      # NEW: For MEM values in MB
    net_data_by_tag  = {}
    netpacket_data_by_tag = {}
    zzzz_map = {}
    node = None
    fallback_date = None
    net_header_parsed = False
    net_columns = []
    netpacket_header_parsed = False
    netpacket_columns = []

    # --- For DISK ---
    diskread_header_parsed = False
    diskread_header = []
    diskread_data_by_tag = {}
    diskwrite_header_parsed = False
    diskwrite_header = []
    diskwrite_data_by_tag = {}
    diskbusy_header_parsed = False
    diskbusy_header = []
    diskbusy_data_by_tag = {}
    diskwait_header_parsed = False
    diskwait_header = []
    diskwait_data_by_tag = {}

    # --- For VG ---
    vgread_header_parsed = False
    vgread_header = []
    vgread_data_by_tag = {}
    vgwrite_header_parsed = False
    vgwrite_header = []
    vgwrite_data_by_tag = {}
    vgbusy_header_parsed = False
    vgbusy_header = []
    vgbusy_data_by_tag = {}
    vgsize_header_parsed = False
    vgsize_header = []
    vgsize_data_by_tag = {}

    # --- For JFSFILE (new) ---
    jfsfile_header_parsed = False
    jfsfile_header = []
    jfsfile_data_by_tag = {}

    # --- NEW: For LPM (BBBR lines) ---
    lpm_data = []
    bbbr_header = []
    bbbr_header_parsed = False

    # --- NEW: For MEMUSE (FS Cache Memory Use data) ---
    # Only lines that start with MEMUSE and whose second field starts with T (e.g., "MEMUSE,T0001")
    memuse_data_by_tag = {}

    # --- NEW: For PAGE (Paging metrics) ---
    # We want to use only lines where the second field starts with T (e.g., "PAGE,T0001")
    page_data_by_tag = {}

    # --- NEW: For SEA (Shared Ethernet Adapter metrics) ---
    # Only lines that start with SEA and whose second field starts with T (e.g., "SEA,T0001")
    sea_header_parsed = False
    sea_header = []
    sea_data_by_tag = {}

    # --- NEW: For SEAPACKET (SEA Packets/s metrics) ---
    # Only lines that start with SEAPACKET and whose second field starts with T (e.g., "SEAPACKET,T0001")
    seapacket_header_parsed = False
    seapacket_header = []
    seapacket_data_by_tag = {}
    # --- NEW: For SEACHPHY (SEA Physical Adapter Errors & Drops) ---
    # Only lines that start with SEACHPHY and whose second field starts with T (e.g., "SEACHPHY,T0001")
    seachphy_header_parsed = False
    seachphy_header = []
    seachphy_data_by_tag = {}


    # --- NEW: For CPU use (per logical CPU) ---
    # This new branch parses lines like "CPU01,Txxxx,User%,Sys%,Wait%,Idle%"
    # and accumulates User% and Sys% per CPU (only if User%+Sys% > 0.05).
    cpu_use_data_by_tag = {}

    base_name = os.path.splitext(os.path.basename(nmon_file))[0]
    file_io_header_parsed = False
    file_io_columns = []

    with open(nmon_file, 'r', encoding='utf-8' , errors='ignore') as f:
        for chunk_lines in read_in_chunks(f):
            for line in chunk_lines:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(',')
                key = parts[0]
                # ZZZZ => timestamps
                if key == 'ZZZZ' and len(parts) >= 4:
                    tag = parts[1]
                    time_str = parts[2]
                    date_str = parts[3].strip()
                    if not re.match(r'^\d{2}-[A-Z]{3}-\d{4}$', date_str.upper()):
                        if fallback_date:
                            date_str = fallback_date
                    zzzz_map[tag] = parse_date_time(date_str, time_str)
                    continue
                # CPU_ALL
                if key == 'CPU_ALL' and len(parts) > 1 and parts[1].startswith('T'):
                    tag = parts[1]
                    try:
                        cpu_data_by_tag[tag] = {
                            'User%': float(parts[2]) if parts[2].strip() else 0.0,
                            'Sys%':  float(parts[3]) if parts[3].strip() else 0.0,
                            'Wait%': float(parts[4]) if parts[4].strip() else 0.0,
                            'Idle%': float(parts[5]) if parts[5].strip() else 0.0
                        }
                    except:
                        pass
                    continue
                # NEW: CPU use per logical core from lines like "CPU01,Txxxx,..."
                if re.match(r'^CPU\d+$', key) and len(parts) > 1 and parts[1].startswith('T'):
                    try:
                        cpu_number = key[len("CPU"):]  # e.g., "01"
                        user_val = float(parts[2]) if parts[2].strip() else 0.0
                        sys_val  = float(parts[3]) if parts[3].strip() else 0.0
                        total = user_val + sys_val
                        if total > 0.05:
                            tag = parts[1]
                            if tag not in cpu_use_data_by_tag:
                                cpu_use_data_by_tag[tag] = {}
                            if cpu_number not in cpu_use_data_by_tag[tag]:
                                cpu_use_data_by_tag[tag][cpu_number] = {"user_sum": 0.0, "sys_sum": 0.0, "count": 0}
                            cpu_use_data_by_tag[tag][cpu_number]["user_sum"] += user_val
                            cpu_use_data_by_tag[tag][cpu_number]["sys_sum"] += sys_val
                            cpu_use_data_by_tag[tag][cpu_number]["count"] += 1
                    except:
                        pass
                    continue
                # LPAR
                if key == 'LPAR' and len(parts) > 1 and parts[1].startswith('T'):
                    tag = parts[1]
                    try:
                        lpar_data_by_tag[tag] = {
                            'PoolCPUs': float(parts[5]) if len(parts) > 5 and parts[5].strip() else 0.0,
                            'PoolIdle': float(parts[8]) if len(parts) > 8 and float(parts[8]) < 300 and parts[8].strip() else 0.0,
                            'PhysicalCPU': float(parts[2]) if parts[2].strip() else 0.0,
                            'VirtualCPUs': float(parts[3]) if parts[3].strip() else 0.0,
                            'Entitled':    float(parts[6]) if len(parts) > 6 and parts[6].strip() else 0.0
                        }
                    except:
                        pass
                    continue
                # PROC
                if key == 'PROC' and len(parts) > 1 and parts[1].startswith('T'):
                    tag = parts[1]
                    try:
                        runnable_val = float(parts[2]) if parts[2].strip() else 0.0
                        swap_in_val  = float(parts[3]) if parts[3].strip() else 0.0
                        pswitch_val  = float(parts[4]) if parts[4].strip() else 0.0
                        syscall_val  = float(parts[5]) if parts[5].strip() else 0.0
                        read_val     = float(parts[6]) if parts[6].strip() else 0.0
                        write_val    = float(parts[7]) if parts[7].strip() else 0.0
                        fork_val     = float(parts[8]) if len(parts) > 8 and parts[8].strip() else 0.0
                        exec_val     = float(parts[9]) if len(parts) > 9 and parts[9].strip() else 0.0
                        sem_val      = float(parts[10]) if len(parts) > 10 and parts[10].strip() else 0.0
                        msg_val      = float(parts[11]) if len(parts) > 11 and parts[11].strip() else 0.0
                        proc_data_by_tag.setdefault(tag, {})
                        proc_data_by_tag[tag].update({
                            'Runnable': runnable_val,
                            'Swap-in':  swap_in_val,
                            'pswitch':  pswitch_val,
                            'Syscall':  syscall_val,
                            'Read':     read_val,
                            'Write':    write_val,
                            'fork':     fork_val,
                            'exec':     exec_val,
                            'sem':      sem_val,
                            'msg':      msg_val
                        })
                    except:
                        pass
                    continue
                # TOP
                if key == 'TOP' and len(parts) > 2:
                    possible_tag = parts[2]
                    if possible_tag.startswith('T'):
                        tag = possible_tag
                        try:
                            cpu_str = parts[3] if len(parts) > 3 else "0"
                            cmd_str = parts[13] if len(parts) > 13 else "?"
                            cpu_val = 0.0
                            if cpu_str.strip():
                                cpu_val = float(cpu_str)
                            # NEW: Add additional fields for the bubble chart:
                            #   - CharIO is taken from field index 10.
                            #   - Memory usage is calculated as the sum of fields at index 8 and 9.
                            chario_val = 0.0
                            mem_usage_val = 0.0
                            if len(parts) > 10:
                                try:
                                    chario_val = float(parts[10])
                                except:
                                    pass
                            if len(parts) > 9:
                                try:
                                    mem_usage_val = float(parts[8]) + float(parts[9])
                                except:
                                    pass
                            top_data_by_tag.setdefault(tag, [])
                            top_data_by_tag[tag].append({
                                '%CPU': cpu_val,
                                'Command': cmd_str,
                                'PID': parts[1],
                                'CharIO': chario_val,
                                'Memory': mem_usage_val
                            })
                        except:
                            pass
                    continue
                # FILE => parse file I/O stats
                if key == 'FILE':
                    if (not file_io_header_parsed) and len(parts) > 2 and "File I/O" in parts[1]:
                        file_io_columns = parts[2:]
                        file_io_header_parsed = True
                        continue
                    if len(parts) > 1 and parts[1].startswith('T') and file_io_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, col_name in enumerate(file_io_columns):
                            d[col_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        file_io_data_by_tag[tag] = d
                    continue
                # MEMNEW
                if key == 'MEMNEW' and len(parts) > 1 and parts[1].startswith('T'):
                    tag = parts[1]
                    try:
                        memnew_data_by_tag[tag] = {
                            'Process%': float(parts[2]) if parts[2].strip() else 0.0,
                            'FScache%': float(parts[3]) if parts[3].strip() else 0.0,
                            'System%':  float(parts[4]) if parts[4].strip() else 0.0,
                            'Free%':    float(parts[5]) if parts[5].strip() else 0.0,
                            'Pinned%':  float(parts[6]) if len(parts) > 6 and parts[6].strip() else 0.0,
                            'User%':    float(parts[7]) if len(parts) > 7 and parts[7].strip() else 0.0
                        }
                    except:
                        pass
                    continue
                # MEM => parse Real/Virtual used% (computed from free%) and add new MB parsing
                if key == 'MEM' and len(parts) > 1 and parts[1].startswith('T'):
                    tag = parts[1]
                    try:
                        # Calculate percentages from free percentages
                        real_free_p = float(parts[2]) if parts[2].strip() else 0.0
                        virt_free_p = float(parts[3]) if parts[3].strip() else 0.0
                        real_used_p = 100.0 - real_free_p
                        virt_used_p = 100.0 - virt_free_p
                        mem_data_by_tag[tag] = {
                            'Real_Used%':    real_used_p,
                            'Virtual_Used%': virt_used_p
                        }
                        # NEW: Parse MB values (columns 4-7)
                        real_free_mb = float(parts[4]) if parts[4].strip() else 0.0
                        virt_free_mb = float(parts[5]) if parts[5].strip() else 0.0
                        real_total_mb = float(parts[6]) if parts[6].strip() else 0.0
                        virt_total_mb = float(parts[7]) if parts[7].strip() else 0.0
                        real_used_mb = real_total_mb - real_free_mb
                        virt_used_mb = virt_total_mb - virt_free_mb
                        mem_mb_data_by_tag[tag] = {
                            'Real_Free_MB': real_free_mb,
                            'Virtual_Free_MB': virt_free_mb,
                            'Real_Total_MB': real_total_mb,
                            'Virtual_Total_MB': virt_total_mb,
                            'Real_Used_MB': real_used_mb,
                            'Virtual_Used_MB': virt_used_mb
                        }
                    except:
                        pass
                    continue
                # NET => parse read/write columns
                if key == 'NET' and len(parts) > 2 and not parts[1].startswith('T'):
                    if not net_header_parsed:
                        net_columns = parts[2:]
                        net_header_parsed = True
                    continue
                if key == 'NET' and len(parts) > 2 and parts[1].startswith('T'):
                    tag = parts[1]
                    numeric_vals = []
                    for x in parts[2:]:
                        try:
                            numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                        except:
                            numeric_vals.append(0.0)
                    d = {}
                    for i, col_name in enumerate(net_columns):
                        d[col_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                    net_data_by_tag[tag] = d
                    continue
                # NETPACKET => parse read/write packet columns
                if key == 'NETPACKET' and len(parts) > 2 and not parts[1].startswith('T'):
                    if not netpacket_header_parsed:
                        netpacket_columns = parts[2:]
                        netpacket_header_parsed = True
                    continue
                if key == 'NETPACKET' and len(parts) > 2 and parts[1].startswith('T'):
                    tag = parts[1]
                    numeric_vals = []
                    for x in parts[2:]:
                        try:
                            numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                        except:
                            numeric_vals.append(0.0)
                    d = {}
                    for i, col_name in enumerate(netpacket_columns):
                        d[col_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                    netpacket_data_by_tag[tag] = d
                    continue
                # AAA => NodeName, date
                if key == 'AAA' and len(parts) > 2:
                    somekey = parts[1]
                    value = parts[2]
                    if somekey == 'NodeName':
                        node = value
                    elif somekey == 'date':
                        fallback_date = value
                # -------------------------
                # DISK READ
                # -------------------------
                if key == 'DISKREAD':
                    if (not diskread_header_parsed) and len(parts) > 2 and not parts[1].startswith('T'):
                        diskread_header = parts[2:]
                        diskread_header_parsed = True
                        continue
                    if len(parts) > 2 and parts[1].startswith('T') and diskread_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, disk_name in enumerate(diskread_header):
                            d[disk_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        diskread_data_by_tag[tag] = d
                    continue
                # DISKWRITE
                if key == 'DISKWRITE':
                    if (not diskwrite_header_parsed) and len(parts) > 2 and not parts[1].startswith('T'):
                        diskwrite_header = parts[2:]
                        diskwrite_header_parsed = True
                        continue
                    if len(parts) > 2 and parts[1].startswith('T') and diskwrite_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, disk_name in enumerate(diskwrite_header):
                            d[disk_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        diskwrite_data_by_tag[tag] = d
                    continue
                # DISKBUSY
                if key == 'DISKBUSY':
                    if (not diskbusy_header_parsed) and len(parts) > 2 and not parts[1].startswith('T'):
                        diskbusy_header = parts[2:]
                        diskbusy_header_parsed = True
                        continue
                    if len(parts) > 2 and parts[1].startswith('T') and diskbusy_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, disk_name in enumerate(diskbusy_header):
                            d[disk_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        diskbusy_data_by_tag[tag] = d
                    continue
                # DISKWAIT
                if key == 'DISKWAIT':
                    if (not diskwait_header_parsed) and len(parts) > 2 and not parts[1].startswith('T'):
                        diskwait_header = parts[2:]
                        diskwait_header_parsed = True
                        continue
                    if len(parts) > 2 and parts[1].startswith('T') and diskwait_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, disk_name in enumerate(diskwait_header):
                            d[disk_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        diskwait_data_by_tag[tag] = d
                    continue
                # -------------------------
                # VG READ
                # -------------------------
                if key == 'VGREAD':
                    if (not vgread_header_parsed) and len(parts) > 2 and not parts[1].startswith('T'):
                        vgread_header = parts[2:]
                        vgread_header_parsed = True
                        continue
                    if len(parts) > 2 and parts[1].startswith('T') and vgread_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, vg_name in enumerate(vgread_header):
                            d[vg_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        vgread_data_by_tag[tag] = d
                    continue
                # VGWRITE
                if key == 'VGWRITE':
                    if (not vgwrite_header_parsed) and len(parts) > 2 and not parts[1].startswith('T'):
                        vgwrite_header = parts[2:]
                        vgwrite_header_parsed = True
                        continue
                    if len(parts) > 2 and parts[1].startswith('T') and vgwrite_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, vg_name in enumerate(vgwrite_header):
                            d[vg_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        vgwrite_data_by_tag[tag] = d
                    continue
                # VGBUSY
                if key == 'VGBUSY':
                    if (not vgbusy_header_parsed) and len(parts) > 2 and not parts[1].startswith('T'):
                        vgbusy_header = parts[2:]
                        vgbusy_header_parsed = True
                        continue
                    if len(parts) > 2 and parts[1].startswith('T') and vgbusy_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, vg_name in enumerate(vgbusy_header):
                            d[vg_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        vgbusy_data_by_tag[tag] = d
                    continue
                # VG SIZE
                if key == 'VGSIZE':
                    if (not vgsize_header_parsed) and len(parts) > 2 and not parts[1].startswith('T'):
                        vgsize_header = parts[2:]
                        vgsize_header_parsed = True
                        continue
                    if len(parts) > 2 and parts[1].startswith('T') and vgsize_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, vg_name in enumerate(vgsize_header):
                            d[vg_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        vgsize_data_by_tag[tag] = d
                    continue
                # -------------------------
                # JFSFILE (new chart)
                # -------------------------
                if key == 'JFSFILE':
                    if (not jfsfile_header_parsed) and len(parts) > 2 and not parts[1].startswith('T'):
                        # Skip the descriptive column and grab the file systems (e.g., '/', '/admin', etc.)
                        jfsfile_header = parts[2:]
                        jfsfile_header_parsed = True
                        continue
                    if len(parts) > 1 and parts[1].startswith('T') and jfsfile_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, fs in enumerate(jfsfile_header):
                            d[fs] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        jfsfile_data_by_tag[tag] = d
                        continue
                # -------------------------
                # NEW: LPM (Live Partition Mobility) events from BBBR lines
                # -------------------------
                if key == 'BBBR':
                    if (not bbbr_header_parsed) and parts[1] == '000':
                        bbbr_header = parts
                        bbbr_header_parsed = True
                        continue
                    if bbbr_header_parsed and re.match(r'0\d{2}', parts[1]):
                        try:
                            # Find indices from header
                            when_idx = bbbr_header.index('when')
                            check_idx = bbbr_header.index('check')
                            pre_idx = bbbr_header.index('pre')
                            post_idx = bbbr_header.index('post')
                            posterror_idx = bbbr_header.index('posterror')
                            old_serial_idx = bbbr_header.index('old_serialno')
                            current_serial_idx = bbbr_header.index('current_serialno')

                            time_str = parts[when_idx]
                            full_timestamp = parse_date_time(fallback_date, time_str) if fallback_date else time_str

                            state = None
                            if len(parts) > check_idx and parts[check_idx] == 'check':
                                state = 'check'
                            elif len(parts) > pre_idx and parts[pre_idx] == 'pre':
                                state = 'pre'
                            elif len(parts) > post_idx and parts[post_idx] == 'post':
                                state = 'post'
                            elif len(parts) > posterror_idx and parts[posterror_idx] == 'posterror':
                                state = 'posterror'

                            if state:
                                old_serial = parts[old_serial_idx]
                                current_serial = parts[current_serial_idx]
                                lpm_data.append({
                                    'timestamp': full_timestamp,
                                    'state': state,
                                    'old_serial': old_serial,
                                    'current_serial': current_serial
                                })
                        except (ValueError, IndexError):
                            # Header not as expected, or data line malformed
                            pass
                    continue
                # -------------------------
                # NEW: MEMUSE (FS Cache Memory Use data)
                # -------------------------
                if key == 'MEMUSE' and len(parts) > 1 and parts[1].startswith('T'):
                    tag = parts[1]
                    try:
                        numperm_val = float(parts[2]) if parts[2].strip() else 0.0
                        minperm_val = float(parts[3]) if parts[3].strip() else 0.0
                        maxperm_val = float(parts[4]) if parts[4].strip() else 0.0
                        memuse_data_by_tag[tag] = {
                            "numperm": numperm_val,
                            "minperm": minperm_val,
                            "maxperm": maxperm_val,
                        }
                    except:
                        pass
                    continue
                # -------------------------
                # NEW: PAGE (Paging metrics)
                # -------------------------
                if key == 'PAGE' and len(parts) > 1 and parts[1].startswith('T'):
                    tag = parts[1]
                    try:
                        # According to the header the columns are:
                        # 0: PAGE, 1: tag, 2: faults, 3: pgin, 4: pgout, 5: pgsin, 6: pgsout, ...
                        pgin  = float(parts[3]) if parts[3].strip() else 0.0
                        pgout = float(parts[4]) if parts[4].strip() else 0.0
                        pgsin = float(parts[5]) if parts[5].strip() else 0.0
                        pgsout= float(parts[6]) if parts[6].strip() else 0.0
                        # Ensure pgout and pgsout are negative:
                        pgout = -abs(pgout)
                        pgsout = -abs(pgsout)
                        page_data_by_tag[tag] = {
                            "pgin": pgin,
                            "pgout": pgout,
                            "pgsin": pgsin,
                            "pgsout": pgsout
                        }
                    except:
                        pass
                    continue
                # -------------------------
                # NEW: SEA (Shared Ethernet Adapter metrics)
                # -------------------------
                # -------------------------
                # NEW: SEACHPHY (SEA PHY Errors & Drops)
                # -------------------------
                if key == 'SEACHPHY':
                    if (not seachphy_header_parsed) and len(parts) > 2 and not parts[1].startswith('T'):
                        seachphy_header = parts[2:]
                        seachphy_header_parsed = True
                        continue
                    if len(parts) > 1 and parts[1].startswith('T') and seachphy_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, col_name in enumerate(seachphy_header):
                            d[col_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        seachphy_data_by_tag[tag] = d
                        continue

                if key == 'SEA':
                    if (not sea_header_parsed) and len(parts) > 2 and not parts[1].startswith('T'):
                        sea_header = parts[2:]
                        sea_header_parsed = True
                        continue
                    if len(parts) > 1 and parts[1].startswith('T') and sea_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, col_name in enumerate(sea_header):
                            d[col_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        sea_data_by_tag[tag] = d
                        continue
                # -------------------------
                # NEW: SEAPACKET (SEA Packets/s metrics)
                # -------------------------
                if key == 'SEAPACKET':
                    if (not seapacket_header_parsed) and len(parts) > 2 and not parts[1].startswith('T'):
                        seapacket_header = parts[2:]
                        seapacket_header_parsed = True
                        continue
                    if len(parts) > 1 and parts[1].startswith('T') and seapacket_header_parsed:
                        tag = parts[1]
                        numeric_vals = []
                        for x in parts[2:]:
                            try:
                                numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                            except:
                                numeric_vals.append(0.0)
                        d = {}
                        for i, col_name in enumerate(seapacket_header):
                            d[col_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        seapacket_data_by_tag[tag] = d
                        continue
    if not node:
        node = base_name
    # Return all parsed data including the new mem_mb_data_by_tag, paging data, sea_data_by_tag, and seapacket_data_by_tag,
    # and now the new cpu_use_data_by_tag.
    return (
        cpu_data_by_tag,
        lpar_data_by_tag,
        proc_data_by_tag,
        file_io_data_by_tag,
        top_data_by_tag,
        zzzz_map,
        node,
        memnew_data_by_tag,
        mem_data_by_tag,
        mem_mb_data_by_tag,  # NEW: MEM MB data
        net_data_by_tag,
        netpacket_data_by_tag,
        diskread_data_by_tag,
        diskwrite_data_by_tag,
        diskbusy_data_by_tag,
        diskwait_data_by_tag,
        vgread_data_by_tag,
        vgwrite_data_by_tag,
        vgbusy_data_by_tag,
        vgsize_data_by_tag,
        jfsfile_data_by_tag,
        memuse_data_by_tag,   # NEW: FS Cache Memory Use data
        page_data_by_tag,     # NEW: Paging data
        sea_data_by_tag,      # NEW: SEA data
        seachphy_data_by_tag,      # NEW: SEA PHY Errors & Drops data
        seapacket_data_by_tag, # NEW: SEA Packets/s data
        cpu_use_data_by_tag,   # NEW: CPU Use per logical CPU data
        lpm_data
    )

################################################################################
# 3. Building NDJSON docs
################################################################################

def build_all_docs(cpu_data_by_tag, lpar_data_by_tag, proc_data_by_tag,
                   file_io_data_by_tag, memnew_data_by_tag, zzzz_map,
                   mem_data_by_tag, net_data_by_tag, netpacket_data_by_tag,
                   diskread_data_by_tag, diskwrite_data_by_tag,
                   diskbusy_data_by_tag, diskwait_data_by_tag,
                   vgread_data_by_tag, vgwrite_data_by_tag,
                   vgbusy_data_by_tag, vgsize_data_by_tag):
    docs = []
    all_tags = (
        set(cpu_data_by_tag.keys())
        | set(lpar_data_by_tag.keys())
        | set(proc_data_by_tag.keys())
        | set(file_io_data_by_tag.keys())
        | set(memnew_data_by_tag.keys())
        | set(mem_data_by_tag.keys())
        | set(net_data_by_tag.keys())
        | set(netpacket_data_by_tag.keys())
        | set(diskread_data_by_tag.keys())
        | set(diskwrite_data_by_tag.keys())
        | set(diskbusy_data_by_tag.keys())
        | set(diskwait_data_by_tag.keys())
        | set(vgread_data_by_tag.keys())
        | set(vgwrite_data_by_tag.keys())
        | set(vgbusy_data_by_tag.keys())
        | set(vgsize_data_by_tag.keys())
        | set(zzzz_map.keys())
    )

    for tag in sorted(all_tags):
        dt = zzzz_map.get(tag)
        if not dt:
            continue
        doc = {"@timestamp": dt}

        if tag in cpu_data_by_tag:
            doc["cpu_all"] = cpu_data_by_tag[tag]
        if tag in lpar_data_by_tag:
            doc["lpar"] = lpar_data_by_tag[tag]
        if tag in proc_data_by_tag:
            doc["proc"] = proc_data_by_tag[tag]
        if tag in file_io_data_by_tag:
            doc["file_io"] = file_io_data_by_tag[tag]
        if tag in memnew_data_by_tag:
            doc["memnew"] = memnew_data_by_tag[tag]
        if tag in mem_data_by_tag:
            doc["mem"] = mem_data_by_tag[tag]
        if tag in net_data_by_tag:
            doc["net"] = net_data_by_tag[tag]
        if tag in netpacket_data_by_tag:
            doc["netpacket"] = netpacket_data_by_tag[tag]

        # add new disk data
        if tag in diskread_data_by_tag:
            doc["diskread"] = diskread_data_by_tag[tag]
        if tag in diskwrite_data_by_tag:
            doc["diskwrite"] = diskwrite_data_by_tag[tag]
        if tag in diskbusy_data_by_tag:
            doc["diskbusy"] = diskbusy_data_by_tag[tag]
        if tag in diskwait_data_by_tag:
            doc["diskwait"] = diskwait_data_by_tag[tag]

        # add new VG data
        if tag in vgread_data_by_tag:
            doc["vgread"] = vgread_data_by_tag[tag]
        if tag in vgwrite_data_by_tag:
            doc["vgwrite"] = vgwrite_data_by_tag[tag]
        if tag in vgbusy_data_by_tag:
            doc["vgbusy"] = vgbusy_data_by_tag[tag]
        if tag in vgsize_data_by_tag:
            doc["vgsize"] = vgsize_data_by_tag[tag]

        if len(doc) > 1:
            docs.append(doc)

    return docs

def build_top_docs(top_data_by_tag, zzzz_map):
    top_docs = []
    for tag, item_list in top_data_by_tag.items():
        dt = zzzz_map.get(tag)
        if not dt:
            continue
        for rec in item_list:
            doc = {"@timestamp": dt}
            doc.update(rec)  # '%CPU', 'Command', 'PID', 'CharIO', and 'Memory'
            top_docs.append(doc)
    return top_docs

def write_ndjson(docs, filepath):
    if not docs:
        return
    with open(filepath, "w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc) + "\n")

################################################################################
# 4. Generate HTML with 16 charts (existing) + 5 new DISK/VG charts (no VG SIZE)
#    + linked zoom (autoscale linking fixed)
#    + NEW: added paging chart, FS Cache Memory Use (numperm) Percentage chart,
#           unstacked SEA (READ/WRITE (KB/s)) chart,
#           SEA Read/Write - Stacked (KB/s) chart,
#           NEW: SEA Packets/s chart,
#           NEW: MEM MB chart,
#           and now 2 new charts for Top 20 Process PIDs by CPU.
#           Also, the new interactive stacked chart "Average Use of Logical CPU Core Threads - POWER=SMT"
#           is inserted immediately after the overall CPU Usage chart.
#
#   Modifications for the new "TOP Commands by %CPU (Stacked)" chart:
#   - A new container is added right after the TOP Commands by %CPU container.
#   - The chartIds array is updated to include "top_cpu_stacked_chart".
#   - A new JavaScript block creates a stacked version of the TOP Commands by %CPU chart.
#
#   NEW: A new graph "InterProcess Comms - Semaphores/s & Message Queues send/s" is added.
#        It is inserted after the "fork() & exec()" graph.
#
#   NEW: A new bubble chart for TOPSUM (mimicking the nmonchart ksh script bubble chart) is added
#        just before the TOP Commands by %CPU chart.
#
#   NEW: A new graph for LPM Migration Events (BBBR) is added after the JFS Percent Full chart.
################################################################################

DASHBOARD_THEME_CSS = r"""
            :root {
                color-scheme: light;
                --bg: #f3efe7;
                --surface: #faf7f1;
                --surface-raised: #fffdf8;
                --surface-subtle: #ebe4d9;
                --text: #23221f;
                --text-soft: #494640;
                --muted: #746e65;
                --border: #d8d0c4;
                --border-strong: #bcb2a4;
                --accent: #c26142;
                --accent-rgb: 194, 97, 66;
                --accent-soft: #efddd3;
                --active-bg: #282724;
                --active-text: #fffaf2;
                --hover: #e9e2d7;
                --success: #627b67;
                --warning: #a87532;
                --danger: #a64f42;
                --chart-1: #c26142;
                --chart-2: #6f665e;
                --chart-3: #87917d;
                --chart-4: #c49a69;
                --chart-5: #8b7062;
                --chart-6: #a88f7f;
                --chart-7: #65796f;
                --chart-8: #b8aa98;
                --chart-inspected: #b9afa2;
                --chart-transferred: #34322e;
                --chart-duration: #c26142;
                --chart-lan: #71816f;
                --chart-lanfree: #b8795f;
                --chart-pct-dedup: #5f7a72;
                --chart-pct-compress: #8c7355;
                --chart-insp-count: #6e7f92;
                --chart-proc: #9a8060;
                --chart-fail: #9b5e55;
                --chart-retr: #7a8470;
                --radius-sm: 3px;
                --radius-md: 6px;
                --space-1: 4px;
                --space-2: 8px;
                --space-3: 12px;
                --space-4: 16px;
                --space-5: 24px;
                --space-6: 32px;
                --font-display: "Iowan Old Style", "Palatino Linotype", Palatino, Georgia, serif;
                --font-ui: Inter, "Helvetica Neue", Helvetica, Arial, sans-serif;
            }

            * {
                box-sizing: border-box;
            }

            html {
                background: var(--bg);
                scroll-behavior: smooth;
            }

            body {
                margin: 0;
                background: var(--bg);
                color: var(--text);
                font-family: var(--font-ui);
                font-size: 12px;
                line-height: 1.4;
                transition: background-color 180ms ease, color 180ms ease;
            }

            body.dark-mode {
                color-scheme: dark;
                --bg: #1c1b18;
                --surface: #24221e;
                --surface-raised: #2a2823;
                --surface-subtle: #312e28;
                --text: #eee7dc;
                --text-soft: #cec4b7;
                --muted: #a79d90;
                --border: #413d36;
                --border-strong: #5a544a;
                --accent: #dd7c5e;
                --accent-rgb: 221, 124, 94;
                --accent-soft: #4a3028;
                --active-bg: #e9dfd1;
                --active-text: #25221e;
                --hover: #35322c;
                --success: #87a18b;
                --warning: #c79a59;
                --danger: #d47b6c;
                --chart-1: #dd7c5e;
                --chart-2: #b5a89b;
                --chart-3: #9eaa91;
                --chart-4: #d2aa77;
                --chart-5: #bc9583;
                --chart-6: #c4aa9b;
                --chart-7: #8fa69a;
                --chart-8: #c7b9a7;
                --chart-inspected: #8f877d;
                --chart-transferred: #e5dbce;
                --chart-duration: #dd7c5e;
                --chart-lan: #91a38f;
                --chart-lanfree: #cf876b;
                --chart-pct-dedup: #7f9a91;
                --chart-pct-compress: #b8976f;
                --chart-insp-count: #8fa0b3;
                --chart-proc: #b89a74;
                --chart-fail: #c48478;
                --chart-retr: #9aa68c;
            }

            button,
            input,
            select {
                font: inherit;
            }

            button,
            [onclick] {
                -webkit-tap-highlight-color: transparent;
            }

            ::selection {
                background: var(--accent-soft);
                color: var(--text);
            }

            .page-shell {
                width: min(100%, 1920px);
                margin: 0 auto;
                padding: 0 clamp(16px, 2.2vw, 42px) var(--space-6);
            }

            .site-header {
                min-height: 148px;
                display: flex;
                align-items: flex-end;
                justify-content: space-between;
                gap: var(--space-6);
                padding: 34px 0 24px;
                border-bottom: 1px solid var(--border-strong);
            }

            .header-logo-container {
                min-width: 0;
            }

            .header-kicker,
            .section-kicker,
            .analytics-kicker {
                display: block;
                margin: 0 0 7px;
                color: var(--accent);
                font-size: 0.66rem;
                font-weight: 700;
                letter-spacing: 0.15em;
                line-height: 1;
                text-transform: uppercase;
            }

            .header-title-row {
                display: flex;
                align-items: baseline;
                flex-wrap: wrap;
                gap: 0.2em;
            }

            .text-spectrum {
                font-family: var(--font-display);
                font-size: clamp(2.35rem, 4vw, 4.7rem);
                font-weight: 400;
                letter-spacing: -0.045em;
                line-height: 0.95;
            }

            .header-description {
                max-width: 650px;
                margin: 14px 0 0;
                color: var(--text-soft);
                font-size: 0.86rem;
            }

            .header-actions {
                display: flex;
                align-items: center;
                gap: var(--space-3);
                padding-bottom: 2px;
                flex-shrink: 0;
            }

            .report-status {
                display: flex;
                align-items: center;
                gap: 7px;
                color: var(--muted);
                font-size: 0.68rem;
                font-weight: 650;
                letter-spacing: 0.08em;
                text-transform: uppercase;
            }

            .report-status::before {
                width: 6px;
                height: 6px;
                border-radius: 50%;
                background: var(--success);
                content: "";
            }

            .theme-toggle {
                position: fixed;
                top: 14px;
                right: 14px;
                z-index: 1200;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                width: 32px;
                height: 32px;
                margin: 0;
                padding: 0;
                border: none;
                border-radius: 999px;
                background: #ededeb;
                color: #111110;
                cursor: pointer;
                box-shadow: none;
                transition: background-color 160ms ease, color 160ms ease, box-shadow 160ms ease;
            }

            .theme-toggle:hover {
                color: #111110;
            }

            .theme-toggle:focus-visible {
                outline: 2px solid var(--accent);
                outline-offset: 2px;
            }

            .theme-toggle svg {
                width: 15px;
                height: 15px;
                display: block;
                flex-shrink: 0;
                stroke: currentColor;
                fill: none;
                stroke-width: 1.5;
                stroke-linecap: round;
                stroke-linejoin: round;
            }

            .theme-toggle .icon-moon {
                display: none;
            }

            body.dark-mode .theme-toggle {
                background: #2a2824;
                color: #fff;
            }

            body.dark-mode .theme-toggle:hover {
                color: #d8d0c4;
            }

            body.dark-mode .theme-toggle .icon-sun {
                display: none;
            }

            body.dark-mode .theme-toggle .icon-moon {
                display: block;
            }

            .section-block {
                margin-top: var(--space-6);
            }

            .section-intro {
                display: flex;
                align-items: flex-end;
                justify-content: space-between;
                gap: var(--space-5);
                margin-bottom: var(--space-4);
            }

            .section-heading {
                margin: 0;
                font-family: var(--font-display);
                font-size: clamp(1.6rem, 2.15vw, 2.5rem);
                font-weight: 400;
                letter-spacing: -0.025em;
                line-height: 1;
            }

            .section-description {
                max-width: 560px;
                margin: 0;
                color: var(--muted);
                font-size: 0.76rem;
                text-align: right;
            }

            .sticky-wrapper {
                position: sticky;
                top: 0;
                z-index: 50;
                margin: 0 calc(clamp(16px, 2.2vw, 42px) * -1);
                padding: 0 clamp(16px, 2.2vw, 42px);
                background: var(--bg);
                border-bottom: 2px solid #000;
                overflow-anchor: none;
            }

            body.dark-mode .sticky-wrapper {
                border-bottom-color: #fff;
            }

            .sticky-wrapper .site-header,
            .sticky-wrapper .analytics-header,
            .sticky-wrapper .analytics-meta,
            .sticky-wrapper .header-kicker,
            .sticky-wrapper .header-description,
            .sticky-wrapper .text-spectrum,
            .sticky-wrapper .header-actions {
                transition:
                    min-height 280ms ease,
                    padding 280ms ease,
                    margin 280ms ease,
                    opacity 220ms ease,
                    max-height 280ms ease,
                    font-size 280ms ease,
                    gap 280ms ease;
            }

            .sticky-wrapper .header-kicker,
            .sticky-wrapper .header-description,
            .sticky-wrapper .analytics-meta {
                overflow: hidden;
                max-height: 6em;
            }

            .sticky-wrapper .header-kicker,
            .sticky-wrapper .analytics-meta {
                max-height: 2em;
            }

            .sticky-wrapper.is-compact .site-header {
                min-height: 0;
                align-items: center;
                gap: var(--space-3);
                padding: 8px 0 6px;
            }

            .sticky-wrapper.is-compact .header-kicker,
            .sticky-wrapper.is-compact .header-description {
                opacity: 0;
                max-height: 0;
                margin: 0;
                pointer-events: none;
            }

            .sticky-wrapper.is-compact .text-spectrum {
                font-size: clamp(1.05rem, 1.6vw, 1.45rem);
                letter-spacing: -0.03em;
                line-height: 1.1;
            }

            .sticky-wrapper.is-compact .header-actions {
                padding-bottom: 0;
            }

            .sticky-wrapper.is-compact .analytics-header {
                padding: 4px 0 8px;
            }

            .sticky-wrapper.is-compact .analytics-meta {
                opacity: 0;
                max-height: 0;
                margin: 0;
                pointer-events: none;
            }

            @media (prefers-reduced-motion: reduce) {
                .sticky-wrapper .site-header,
                .sticky-wrapper .analytics-header,
                .sticky-wrapper .analytics-meta,
                .sticky-wrapper .header-kicker,
                .sticky-wrapper .header-description,
                .sticky-wrapper .text-spectrum,
                .sticky-wrapper .header-actions {
                    transition: none;
                }
            }

            .analytics-header {
                border-top: 0;
                padding: 10px 0 11px;
            }

            .analytics-meta {
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: var(--space-3);
                margin-bottom: 8px;
            }

            .analytics-kicker {
                margin: 0;
                color: var(--muted);
            }

            .analytics-context {
                color: var(--muted);
                font-size: 0.67rem;
                letter-spacing: 0.06em;
                text-transform: uppercase;
            }

            .chart-box,
            .chart-container {
                display: flex;
                min-width: 0;
                min-height: 0;
                flex-direction: column;
                overflow: hidden;
                padding: 8px;
                border: 1px solid var(--border);
                border-radius: var(--radius-md);
                background: var(--surface);
            }

            .chart-container {
                height: 400px;
            }

            .chart-container > div {
                width: 100%;
                height: 100%;
                min-height: 0;
            }

            .chart-container p {
                margin: 0;
                padding: 20px;
                color: var(--muted);
                font-size: 0.68rem;
                text-align: center;
            }

            .chart-header {
                display: flex;
                min-height: 28px;
                align-items: flex-start;
                justify-content: space-between;
                gap: var(--space-3);
                margin-bottom: 10px;
                padding-bottom: 9px;
                border-bottom: 1px solid var(--border);
            }

            .chart-title {
                min-width: 0;
                overflow: hidden;
                color: var(--text);
                font-family: var(--font-display);
                font-size: 0.88rem;
                font-weight: 400;
                letter-spacing: -0.01em;
                line-height: 1.2;
                text-overflow: ellipsis;
                white-space: nowrap;
            }

            .lf-toggle {
                display: inline-flex;
                max-width: 100%;
                overflow: hidden;
                border: 1px solid var(--border);
                border-radius: var(--radius-sm);
                background: var(--surface-raised);
            }

            .lf-btn {
                display: inline-flex;
                min-height: 23px;
                align-items: center;
                justify-content: center;
                padding: 2px 8px;
                border: 0;
                border-right: 1px solid var(--border);
                background: transparent;
                color: var(--muted);
                cursor: pointer;
                font-size: 0.65rem;
                font-weight: 650;
                letter-spacing: 0.02em;
                line-height: 1;
                user-select: none;
                white-space: nowrap;
                transition: background-color 120ms ease, color 120ms ease;
            }

            .lf-btn:last-child {
                border-right: 0;
            }

            .lf-btn:hover {
                background: var(--hover);
                color: var(--text);
            }

            .lf-btn.active {
                background: var(--active-bg);
                color: var(--active-text);
            }

            .filter-panel {
                margin-top: 0;
                padding: 8px 0 4px;
                border-top: 0;
                border-bottom: 0;
            }

            .filter-row-container {
                display: flex;
                align-items: flex-end;
                gap: 11px;
                flex-wrap: wrap;
            }

            .chart-filter-label {
                align-self: center;
                margin-right: 7px;
                color: var(--text);
                font-family: var(--font-display);
                font-size: 0.95rem;
                white-space: nowrap;
            }

            .filter-divider {
                width: 1px;
                height: 31px;
                margin: 0 2px;
                align-self: flex-end;
                background: var(--border);
            }

            .filter-group {
                display: flex;
                min-width: 0;
                flex-direction: column;
            }

            .filter-group label,
            .control-label {
                margin-bottom: 4px;
                color: var(--muted);
                font-size: 0.59rem;
                font-weight: 700;
                letter-spacing: 0.08em;
                line-height: 1;
                text-transform: uppercase;
            }

            input[type="date"],
            input[type="time"],
            input[type="text"],
            input[type="number"],
            select,
            .ms-trigger {
                height: 29px;
                border: 1px solid var(--border);
                border-radius: var(--radius-sm);
                background: var(--surface-raised);
                color: var(--text);
                outline: 0;
                transition: border-color 120ms ease, background-color 120ms ease;
            }

            input[type="date"],
            input[type="time"] {
                width: 116px;
                padding: 3px 6px;
                font-size: 0.69rem;
            }

            input[type="text"] {
                width: 160px;
                padding: 3px 8px;
                font-size: 0.69rem;
            }

            select {
                min-width: 72px;
                max-width: 220px;
                padding: 3px 8px;
                font-size: 0.69rem;
            }

            input:focus,
            select:focus {
                border-color: var(--accent);
            }

            .filter-actions {
                display: flex;
                align-items: center;
                justify-content: flex-end;
                gap: 6px;
                flex: 1 1 140px;
                margin-left: auto;
            }

            button.btn-reset,
            .tf-btn-action,
            .tf-btn-mode {
                min-height: 29px;
                padding: 4px 10px;
                border: 1px solid var(--border);
                border-radius: var(--radius-sm);
                background: var(--surface-raised);
                color: var(--text-soft);
                cursor: pointer;
                font-size: 0.65rem;
                font-weight: 650;
                letter-spacing: 0.015em;
                transition: background-color 120ms ease, border-color 120ms ease, color 120ms ease;
            }

            button.btn-reset:hover,
            .tf-btn-action:hover,
            .tf-btn-mode:hover {
                border-color: var(--border-strong);
                background: var(--hover);
                color: var(--text);
            }

            button.btn-primary,
            .tf-btn-action.tf-apply {
                border-color: var(--active-bg);
                background: var(--active-bg);
                color: var(--active-text);
            }

            button.btn-accent {
                border-color: var(--accent);
                background: var(--accent);
                color: #fffaf2;
            }

            .empty-state {
                padding: 20px;
                color: var(--muted);
                font-size: 0.68rem;
                text-align: center;
            }

            svg text {
                font-family: var(--font-ui);
            }

            #chartsContainer {
                display: grid;
                gap: var(--space-3);
                width: 100%;
                margin-top: 0;
            }

            #chartsContainer::after {
                content: none;
            }

            .chart-container.fullscreen {
                position: fixed !important;
                top: 0 !important;
                left: 0 !important;
                width: 100vw !important;
                height: 100vh !important;
                z-index: 3000;
                margin: 0;
                padding: 16px;
                border: none !important;
                border-radius: 0;
                background: var(--bg);
            }

            .chart-container.hidden {
                display: none !important;
            }

            body.fullscreen-mode .sticky-wrapper,
            body.fullscreen-mode .theme-toggle,
            body.fullscreen-mode .section-intro {
                display: none !important;
            }

            #menu_b {
                margin-top: 8px;
                padding-top: 12px;
                border-top: 1px dashed var(--border);
            }

            .js-plotly-plot .plotly .modebar {
                left: auto !important;
                right: 8px !important;
                top: 4px !important;
                background: transparent !important;
            }

            .js-plotly-plot .plotly .modebar-btn path {
                fill: var(--muted) !important;
            }

            .js-plotly-plot .plotly .modebar-btn:hover path {
                fill: var(--text) !important;
            }

            .js-plotly-plot .plotly .modebar-btn.active path {
                fill: var(--accent) !important;
            }

            @media (max-width: 1280px) {
                .site-header {
                    min-height: 130px;
                    padding-top: 26px;
                }

                .section-description {
                    max-width: 430px;
                }
            }

            @media (max-width: 900px) {
                .site-header {
                    align-items: flex-start;
                    flex-direction: column;
                    gap: var(--space-4);
                    padding: 24px 0 18px;
                }

                .header-actions {
                    width: 100%;
                    justify-content: space-between;
                }

                .section-intro {
                    align-items: flex-start;
                    flex-direction: column;
                    gap: 8px;
                }

                .section-description {
                    max-width: none;
                    text-align: left;
                }

                .filter-divider {
                    display: none;
                }
            }

            @media (max-width: 560px) {
                .page-shell {
                    padding-right: 12px;
                    padding-left: 12px;
                }

                .sticky-wrapper {
                    margin-right: -12px;
                    margin-left: -12px;
                    padding-right: 12px;
                    padding-left: 12px;
                }

                .text-spectrum {
                    font-size: 2.45rem;
                }

                .sticky-wrapper.is-compact .text-spectrum {
                    font-size: 1.05rem;
                }

                .report-status {
                    display: none;
                }

                .filter-row-container {
                    gap: 9px;
                }

                .filter-actions {
                    width: 100%;
                    flex-basis: 100%;
                    justify-content: flex-start;
                    margin-top: 3px;
                }
            }

            @media print {
                body {
                    background: #fff;
                }

                .header-actions,
                .theme-toggle,
                .filter-panel {
                    display: none !important;
                }

                .sticky-wrapper {
                    position: static !important;
                }

                .sticky-wrapper .site-header,
                .sticky-wrapper .analytics-header,
                .sticky-wrapper .analytics-meta,
                .sticky-wrapper .header-kicker,
                .sticky-wrapper .header-description,
                .sticky-wrapper .text-spectrum,
                .sticky-wrapper .header-actions {
                    transition: none !important;
                }

                .sticky-wrapper.is-compact .site-header {
                    min-height: 148px;
                    align-items: flex-end;
                    gap: var(--space-6);
                    padding: 34px 0 24px;
                }

                .sticky-wrapper.is-compact .header-kicker,
                .sticky-wrapper.is-compact .header-description,
                .sticky-wrapper.is-compact .analytics-meta {
                    opacity: 1 !important;
                    max-height: none !important;
                    margin: revert !important;
                    pointer-events: auto !important;
                }

                .sticky-wrapper.is-compact .header-kicker {
                    margin: 0 0 7px !important;
                }

                .sticky-wrapper.is-compact .header-description {
                    margin: 14px 0 0 !important;
                }

                .sticky-wrapper.is-compact .analytics-meta {
                    margin-bottom: 8px !important;
                }

                .sticky-wrapper.is-compact .text-spectrum {
                    font-size: clamp(2.35rem, 4vw, 4.7rem);
                    letter-spacing: -0.045em;
                    line-height: 0.95;
                }

                .sticky-wrapper.is-compact .analytics-header {
                    padding: 10px 0 11px;
                }
            }
"""

def generate_html_page(lpar_data_map, top_data_map, frame_map, lpm_data_map, output_html):
    """
    Original charts: 16 charts + 5 new DISK/VG charts.
    Added:
      (1) Linked Zoom: any zoom/pan on one chart auto-updates all others.
      (2) The Reset Zoom button is removed.
      (3) NEW: A new line chart "FS Cache Memory Use (numperm) Percentage" is added after the TOP Commands by %CPU chart.
      (4) NEW: Paging chart ("All Paging per second") is added after the Swap-in plot.
      (5) NEW: SEA (READ/WRITE (KB/s)) chart is added.
      (6) NEW: SEA Read/Write - Stacked (KB/s) chart is added.
      (7) NEW: SEA Packets/s chart is added.
      (8) NEW: A new MEM MB chart is added after the Memory Usage (MEMNEW) chart.
      (9) NEW: Two new charts "Top 20 Process PIDs by CPU" (unstacked) and 
             "Top 20 Process PIDs by CPU (Stacked)" are added just after the TOP Commands chart.
      (10) NEW: A new stacked chart "TOP Commands by %CPU (Stacked)" is added immediately
             after the TOP Commands by %CPU chart.
      (11) NEW: A new interactive stacked chart "Average Use of Logical CPU Core Threads - POWER=SMT"
             is added immediately after the overall CPU Usage chart.
      (12) NEW: A new graph "InterProcess Comms - Semaphores/s & Message Queues send/s" is added
             immediately after the fork() & exec() chart.
      (13) NEW: A new bubble chart for TOPSUM (Total CPU, Char I/O, Max Memory per Command)
             is added just before the TOP Commands by %CPU chart.
      (14) NEW: A new point chart for LPM Migration Events is added after the JFS Percent Full chart.
    """
    embedded_all = json.dumps(lpar_data_map)
    embedded_top = json.dumps(top_data_map)
    embedded_lpm = json.dumps(lpm_data_map)
    embedded_frames = json.dumps(frame_map)

    html_content = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light dark">
  <title>nmon2plotly · Performance Analytics</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
  <style>
{DASHBOARD_THEME_CSS}
  </style>
</head>
<body>
  <script>
  (function () {{
    var theme = 'light';
    try {{
      var stored = localStorage.getItem('dashboard-theme');
      if (stored === 'system' || stored === 'light' || stored === 'dark') theme = stored;
    }} catch (e) {{}}
    var dark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches);
    document.body.classList.toggle('dark-mode', dark);
  }})();
  </script>
  <button type="button" class="theme-toggle" id="modeToggle" aria-label="Switch to dark theme" title="Light" aria-pressed="false">
      <svg class="icon-sun" viewBox="0 0 24 24" aria-hidden="true">
          <circle cx="12" cy="12" r="3.25"></circle>
          <path d="M12 2.75v2.1M12 19.15v2.1M2.75 12h2.1M19.15 12h2.1M5.64 5.64l1.48 1.48M16.88 16.88l1.48 1.48M5.64 18.36l1.48-1.48M16.88 7.12l1.48-1.48"></path>
      </svg>
      <svg class="icon-moon" viewBox="0 0 24 24" aria-hidden="true">
          <path d="M15.4 3.6a8.7 8.7 0 1 0 4.95 15.55A7.05 7.05 0 0 1 15.4 3.6z"></path>
      </svg>
  </button>
  <div class="page-shell">
    <div id="dashboard-sticky-top" class="sticky-wrapper">
      <header class="site-header">
        <div class="header-logo-container">
          <span class="header-kicker">Infrastructure intelligence</span>
          <div class="header-title-row">
            <span class="text-spectrum">nmon2plotly</span>
          </div>
          <p class="header-description">A focused view of CPU, memory, disk, network and adapter health across the active estate.</p>
        </div>
        <div class="header-actions">
          <span class="report-status">Live nmon session</span>
          <div class="lf-toggle">
            <button type="button" class="lf-btn" id="compareToggle" aria-pressed="false" title="Compare two LPARs">Compare</button>
          </div>
        </div>
      </header>
      <section class="analytics-header" aria-label="Filtered performance summary">
        <div class="analytics-meta">
          <span class="analytics-kicker">Filtered estate</span>
          <span class="analytics-context" id="analytics-context">Select a frame and LPAR</span>
        </div>
        <div class="menu filter-panel" aria-label="Chart filters">
          <div class="filter-row-container">
            <span class="chart-filter-label">Chart filters</span>
            <div class="filter-group">
              <label for="frame_select">Frame</label>
              <select id="frame_select"></select>
            </div>
            <div class="filter-group">
              <label for="lpar_select">LPAR</label>
              <select id="lpar_select"></select>
            </div>
            <div class="filter-group">
              <label for="start_date">Start date</label>
              <input type="date" id="start_date">
            </div>
            <div class="filter-group">
              <label for="end_date">End date</label>
              <input type="date" id="end_date">
            </div>
            <div class="filter-divider" aria-hidden="true"></div>
            <div class="filter-group">
              <label for="chartsPerRow">Charts per row</label>
              <select id="chartsPerRow">
                <option value="1">1</option>
                <option value="2">2</option>
                <option value="3" selected>3</option>
                <option value="4">4</option>
                <option value="5">5</option>
                <option value="6">6</option>
                <option value="7">7</option>
                <option value="8">8</option>
                <option value="9">9</option>
                <option value="10">10</option>
                <option value="11">11</option>
                <option value="12">12</option>
                <option value="13">13</option>
                <option value="14">14</option>
                <option value="15">15</option>
                <option value="16">16</option>
                <option value="17">17</option>
                <option value="18">18</option>
                <option value="19">19</option>
                <option value="20">20</option>
                <option value="21">21</option>
                <option value="22">22</option>
                <option value="23">23</option>
              </select>
            </div>
            <div class="filter-group">
              <label for="lpar_search">Search LPAR</label>
              <input type="text" id="lpar_search" list="lpar_datalist" placeholder="Type LPAR name" />
              <datalist id="lpar_datalist"></datalist>
            </div>
            <div class="filter-actions">
              <button class="btn-reset btn-primary" type="button" onclick="applyFilter()">Filter</button>
            </div>
          </div>
        </div>
      </section>
    </div>
    <script>
    (function () {{
        var stickyTop = document.getElementById('dashboard-sticky-top');
        if (!stickyTop || stickyTop.dataset.compactScrollBound === '1') return;
        stickyTop.dataset.compactScrollBound = '1';

        var COMPACT_ON = 56;
        var EXPAND_ON = 18;
        var ticking = false;
        var settling = false;
        var settleTimer = 0;
        var lastY = window.scrollY || window.pageYOffset || 0;

        function setCompact(compact) {{
            if (stickyTop.classList.contains('is-compact') === compact) return;
            stickyTop.classList.toggle('is-compact', compact);
            settling = true;
            window.clearTimeout(settleTimer);
            settleTimer = window.setTimeout(function () {{
                settling = false;
                lastY = window.scrollY || window.pageYOffset || 0;
            }}, 320);
        }}

        function syncCompactState() {{
            ticking = false;
            if (settling) return;

            var y = window.scrollY || window.pageYOffset || 0;
            var goingDown = y > lastY + 1;
            var goingUp = y < lastY - 1;
            var isCompact = stickyTop.classList.contains('is-compact');

            if (y <= EXPAND_ON) {{
                setCompact(false);
            }} else if (y >= COMPACT_ON && (goingDown || !goingUp)) {{
                setCompact(true);
            }} else if (goingUp && y < COMPACT_ON) {{
                setCompact(false);
            }} else if (!isCompact && y >= COMPACT_ON) {{
                setCompact(true);
            }}

            lastY = y;
        }}

        function onScroll() {{
            if (!ticking) {{
                ticking = true;
                window.requestAnimationFrame(syncCompactState);
            }}
        }}

        window.addEventListener('scroll', onScroll, {{ passive: true }});
        window.addEventListener('beforeprint', function () {{
            setCompact(false);
        }});
        syncCompactState();
    }})();
    </script>
    <main>
      <section class="section-block" aria-labelledby="overview-heading">
        <div class="section-intro">
          <div>
            <span class="section-kicker">Performance landscape</span>
            <h1 class="section-heading" id="overview-heading">Estate overview</h1>
          </div>
          <p class="section-description">Compare CPU, memory, I/O and adapter health without losing the operational detail behind each sample.</p>
        </div>
        <div id="chartsContainer">
    <div class="chart-container"><div id="cpu_usage_chart"></div></div>
    <div class="chart-container"><div id="cpu_use_chart"></div></div>
    <div class="chart-container"><div id="lpar_usage_chart"></div></div>
    <div class="chart-container"><div id="pool_usage_chart"></div></div>
    <div class="chart-container"><div id="runnable_chart"></div></div>
    <div class="chart-container"><div id="syscall_chart"></div></div>
    <div class="chart-container"><div id="pswitch_chart"></div></div>
    <div class="chart-container"><div id="fork_exec_chart"></div></div>
    <div class="chart-container"><div id="sem_msg_chart"></div></div>
    <div class="chart-container"><div id="fileio_chart"></div></div>
    <div class="chart-container"><div id="top_bubble_chart"></div></div>
    <div class="chart-container"><div id="top_cpu_chart"></div></div>
    <div class="chart-container"><div id="top_cpu_stacked_chart"></div></div>
    <div class="chart-container"><div id="top_pid_bubble_chart"></div></div>
    <div class="chart-container"><div id="top_pid_chart"></div></div>
    <div class="chart-container"><div id="top_pid_stacked_chart"></div></div>
    <div class="chart-container"><div id="fs_cache_chart"></div></div>
    <div class="chart-container"><div id="memnew_chart"></div></div>
    <div class="chart-container"><div id="mem_mb_chart"></div></div>
    <div class="chart-container"><div id="memused_chart"></div></div>
    <div class="chart-container"><div id="swapin_chart"></div></div>
    <div class="chart-container"><div id="paging_chart"></div></div>
    <div class="chart-container"><div id="net_chart"></div></div>
    <div class="chart-container"><div id="net_stacked_chart"></div></div>
    <div class="chart-container"><div id="netpacket_chart"></div></div>
    <div class="chart-container"><div id="netsize_chart"></div></div>
    <div class="chart-container"><div id="net_error_chart"></div></div>
    <div class="chart-container"><div id="fc_chart"></div></div>
    <div class="chart-container"><div id="fc_summary_chart"></div></div>
    <div class="chart-container"><div id="fc_stacked_chart"></div></div>
    <div class="chart-container"><div id="fcxfer_chart"></div></div>
    <div class="chart-container"><div id="fc_san_chart"></div></div>
    <div class="chart-container"><div id="fc_stacked_san_chart"></div></div>
    <div class="chart-container"><div id="disk_read_write_chart"></div></div>
    <div class="chart-container"><div id="disk_read_write_stacked_chart"></div></div>
    <div class="chart-container"><div id="disk_busy_chart"></div></div>
    <div class="chart-container"><div id="disk_wait_chart"></div></div>
    <div class="chart-container"><div id="vg_read_write_chart"></div></div>
    <div class="chart-container"><div id="vg_read_write_stacked_chart"></div></div>
    <div class="chart-container"><div id="vg_busy_chart"></div></div>
    <div class="chart-container"><div id="jfs_percent_full_chart"></div></div>
    <div class="chart-container"><div id="lpm_chart"></div></div>
    <div class="chart-container"><div id="sea_chart"></div></div>
    <div class="chart-container"><div id="sea_summary_chart"></div></div>
    <div class="chart-container"><div id="sea_stacked_chart"></div></div>
    <div class="chart-container"><div id="sea_packet_chart"></div></div>

<div class="chart-container"><div id="sea_phy_rw_chart"></div></div>
<div class="chart-container"><div id="sea_phy_summary_chart"></div></div>

    <div class="chart-container"><div id="sea_phy_error_chart"></div></div>
    <div class="chart-container"><div id="sea_phy_drop_chart"></div></div>
        </div>
      </section>
    </main>
  </div>
  <script>
    const lparDataMap = {embedded_all};
    const topDataMap  = {embedded_top};
    const lpmDataMap  = {embedded_lpm};
    const frameMap    = {embedded_frames};

    function themeColor(name) {{
      return getComputedStyle(document.body).getPropertyValue(name).trim();
    }}

    function themeRgba(name, alpha) {{
      const hex = (themeColor(name) || '').replace('#', '');
      if (hex.length < 6) return themeColor(name);
      const r = parseInt(hex.slice(0, 2), 16);
      const g = parseInt(hex.slice(2, 4), 16);
      const b = parseInt(hex.slice(4, 6), 16);
      return 'rgba(' + r + ', ' + g + ', ' + b + ', ' + alpha + ')';
    }}

    function readPlotlyTheme() {{
      return {{
        paper: themeColor('--surface'),
        plot: themeColor('--surface'),
        text: themeColor('--text'),
        muted: themeColor('--muted'),
        border: themeColor('--border'),
        fontUi: themeColor('--font-ui'),
        fontDisplay: themeColor('--font-display'),
        activeBg: themeColor('--active-bg'),
        activeText: themeColor('--active-text'),
        borderStrong: themeColor('--border-strong'),
        colorway: [
          themeColor('--chart-1'), themeColor('--chart-2'), themeColor('--chart-3'),
          themeColor('--chart-4'), themeColor('--chart-5'), themeColor('--chart-6'),
          themeColor('--chart-7'), themeColor('--chart-8')
        ]
      }};
    }}

    function applyPlotlyTheme(layout) {{
      const t = readPlotlyTheme();
      const next = Object.assign({{}}, layout || {{}});
      next.paper_bgcolor = t.paper;
      next.plot_bgcolor = t.plot;
      next.colorway = t.colorway;
      next.font = Object.assign({{ family: t.fontUi, color: t.text, size: 11 }}, next.font || {{}});
      next.hoverlabel = Object.assign({{
        bgcolor: t.activeBg,
        bordercolor: t.borderStrong,
        font: {{ family: t.fontUi, color: t.activeText, size: 11 }}
      }}, next.hoverlabel || {{}});
      next.legend = Object.assign({{
        bgcolor: 'rgba(0,0,0,0)',
        borderwidth: 0,
        font: {{ family: t.fontUi, color: t.muted, size: 10 }}
      }}, next.legend || {{}});
      if (typeof next.title === 'string') {{
        next.title = {{ text: next.title, font: {{ family: t.fontDisplay, size: 15, color: t.text }}, x: 0.02, xanchor: 'left' }};
      }} else if (next.title && typeof next.title === 'object') {{
        next.title = Object.assign({{}}, next.title);
        next.title.font = Object.assign({{ family: t.fontDisplay, size: 15, color: t.text }}, next.title.font || {{}});
        if (next.title.x === undefined) {{
          next.title.x = 0.02;
          next.title.xanchor = 'left';
        }}
      }}
      const axisDefaults = {{
        color: t.muted,
        gridcolor: t.border,
        linecolor: t.border,
        zerolinecolor: t.border,
        tickfont: {{ color: t.muted, family: t.fontUi, size: 10 }},
        titlefont: {{ color: t.muted, family: t.fontUi, size: 11 }}
      }};
      ['xaxis', 'yaxis', 'xaxis2', 'yaxis2'].forEach(axis => {{
        if (next[axis] || axis === 'xaxis' || axis === 'yaxis') {{
          const current = next[axis] || {{}};
          const merged = Object.assign({{}}, axisDefaults, current);
          if (current.titlefont) merged.titlefont = Object.assign({{}}, axisDefaults.titlefont, current.titlefont);
          if (current.tickfont) merged.tickfont = Object.assign({{}}, axisDefaults.tickfont, current.tickfont);
          next[axis] = merged;
        }}
      }});
      next.margin = Object.assign({{ l: 52, r: 18, t: 52, b: 42 }}, next.margin || {{}});
      return next;
    }}

    function plotlyThemeRelayout() {{
      const t = readPlotlyTheme();
      return {{
        'plot_bgcolor': t.plot,
        'paper_bgcolor': t.paper,
        'font.color': t.text,
        'font.family': t.fontUi,
        'xaxis.color': t.muted,
        'xaxis.gridcolor': t.border,
        'xaxis.linecolor': t.border,
        'xaxis.zerolinecolor': t.border,
        'yaxis.color': t.muted,
        'yaxis.gridcolor': t.border,
        'yaxis.linecolor': t.border,
        'yaxis.zerolinecolor': t.border,
        'title.font.color': t.text,
        'title.font.family': t.fontDisplay,
        'legend.font.color': t.muted,
        'legend.bgcolor': 'rgba(0,0,0,0)',
        'colorway': t.colorway
      }};
    }}

    function traceHasOnlyZeroValues(trace) {{
      if (!trace) {{
        return false;
      }}
      const arraysToInspect = [];
      if (Array.isArray(trace.y)) {{
        arraysToInspect.push(trace.y);
      }}
      if (Array.isArray(trace.values)) {{
        arraysToInspect.push(trace.values);
      }}
      if (trace.marker && Array.isArray(trace.marker.size)) {{
        arraysToInspect.push(trace.marker.size);
      }}
      if (!arraysToInspect.length) {{
        return false;
      }}
      let numericCount = 0;
      let numericNonZero = 0;
      let nonNullCount = 0;
      arraysToInspect.forEach(arr => {{
        if (!Array.isArray(arr)) {{
          return;
        }}
        arr.forEach(val => {{
          if (val !== null && val !== undefined) {{
            nonNullCount++;
          }}
          if (typeof val === 'number' && isFinite(val)) {{
            numericCount++;
            if (Math.abs(val) > 1e-9) {{
              numericNonZero++;
            }}
          }}
        }});
      }});
      if (numericCount > 0) {{
        return numericNonZero === 0;
      }}
      return nonNullCount === 0;
    }}

    const originalPlotlyNewPlot = Plotly.newPlot;
    Plotly.newPlot = function(div, data, layout, config) {{
      if (Array.isArray(data)) {{
        data.forEach(trace => {{
          if (!trace || trace.visible !== undefined) {{
            return;
          }}
          if (traceHasOnlyZeroValues(trace)) {{
            trace.visible = 'legendonly';
          }}
        }});
      }}
      layout = applyPlotlyTheme(layout || {{}});
      config = Object.assign({{ displaylogo: false, responsive: true }}, config || {{}});
      return originalPlotlyNewPlot.call(this, div, data, layout, config);
    }};

    // --- MODIFICATION EXPLICITE : PARTIE 1 ---
    // Ce tableau 'chartIds' est la liste principale de tous les graphiques.
    // La fonction de synchronisation l'utilise pour savoir quels graphiques mettre à jour.
    const chartIds = [
      "cpu_usage_chart",
      "cpu_use_chart",
      "lpar_usage_chart",
      "pool_usage_chart",
      "runnable_chart",
      "syscall_chart",
      "pswitch_chart",
      "fork_exec_chart",
      "sem_msg_chart",
      "fileio_chart",
      "top_bubble_chart",
      "top_cpu_chart",
      "top_cpu_stacked_chart",
      "top_pid_bubble_chart",
      "top_pid_chart",
      "top_pid_stacked_chart",
      "fs_cache_chart",
      "memnew_chart",
      "mem_mb_chart",
      "memused_chart",
      "swapin_chart",
      "paging_chart",
      "net_chart",
      "net_stacked_chart",
      "netpacket_chart",
      "netsize_chart",
      "net_error_chart",
      "fc_chart",
      "fc_summary_chart",
      "fc_stacked_chart",
      "fcxfer_chart",
      "fc_san_chart",
      "fc_stacked_san_chart",
      "disk_read_write_chart",
      "disk_read_write_stacked_chart",
      "disk_busy_chart",
      "disk_wait_chart",
      "vg_read_write_chart",
      "vg_read_write_stacked_chart",
      "vg_busy_chart",
      "jfs_percent_full_chart",
      "lpm_chart",
      "sea_chart",
      "sea_summary_chart",
      "sea_stacked_chart",
      "sea_packet_chart",
      "sea_phy_rw_chart",
      "sea_phy_summary_chart",
      "sea_phy_error_chart",
      "sea_phy_drop_chart",
    ];
    function applyDarkModeToAllCharts() {{
      const layoutUpdates = plotlyThemeRelayout();
      chartIds.forEach(id => {{
        const el = document.getElementById(id);
        if (el && el.data) {{
          Plotly.relayout(el, layoutUpdates);
        }}
      }});
    }}

    const toggle = document.getElementById('modeToggle');
    const themeMedia = window.matchMedia('(prefers-color-scheme: dark)');
    const THEME_KEY = 'dashboard-theme';

    function readStoredTheme() {{
      try {{
        const stored = localStorage.getItem(THEME_KEY);
        if (stored === 'system' || stored === 'light' || stored === 'dark') return stored;
      }} catch (e) {{}}
      return 'light';
    }}

    function themeIsDark(theme) {{
      return theme === 'dark' || (theme === 'system' && themeMedia.matches);
    }}

    function applyTheme(theme) {{
      const dark = themeIsDark(theme);
      document.body.classList.toggle('dark-mode', dark);
      if (!toggle) return;
      toggle.setAttribute('aria-pressed', dark ? 'true' : 'false');
      toggle.setAttribute('aria-label', dark ? 'Switch to light theme' : 'Switch to dark theme');
      toggle.setAttribute('title', dark ? 'Dark' : 'Light');
    }}

    function persistTheme(theme) {{
      try {{ localStorage.setItem(THEME_KEY, theme); }} catch (e) {{}}
      applyTheme(theme);
    }}

    if (toggle) {{
      toggle.addEventListener('click', () => {{
        persistTheme(document.body.classList.contains('dark-mode') ? 'light' : 'dark');
        renderCharts();
      }});
    }}

    if (themeMedia.addEventListener) {{
      themeMedia.addEventListener('change', () => applyTheme(readStoredTheme()));
    }} else if (themeMedia.addListener) {{
      themeMedia.addListener(() => applyTheme(readStoredTheme()));
    }}

    applyTheme(readStoredTheme());
    
  // remember where we were scrolled, so we can restore on fullscreen exit
     let lastScrollTop = 0;
     
    // Fullâ€‘screen toggle on doubleâ€‘click
function setupFullscreen() {{
  const containers = document.querySelectorAll('.chart-container');
  containers.forEach(container => {{
  container.addEventListener('dblclick', () => {{
  
  // stash scroll before we toggle into fullscreen
       const willEnter = !document.body.classList.contains('fullscreen-mode');
       if (willEnter) lastScrollTop = window.scrollY;
       const isFS = document.body.classList.toggle('fullscreen-mode');

      if (isFS) {{
        // hide siblings, expand this one
        containers.forEach(c => {{ if (c !== container) c.classList.add('hidden'); }});
        container.classList.add('fullscreen');
        Plotly.Plots.resize(container.firstElementChild);
      }} 
      else {{
        // restore layout
        containers.forEach(c => c.classList.remove('hidden','fullscreen'));
        chartIds.forEach(id => Plotly.Plots.resize(document.getElementById(id)));
         // restore scroll so this chart stays in view
        window.scrollTo(0, lastScrollTop);
      }}
    }});
  }});
}}
    
    // --- MODIFICATION EXPLICITE : PARTIE 2 ---
    // La variable 'relayoutLock' et la fonction 'linkCharts' forment le cœur de la synchronisation.
    // Cette fonction écoute les événements de zoom/pan sur un graphique et applique
    // les changements à tous les autres graphiques listés dans 'chartIds'.
    
    // Variable pour éviter les boucles d'événements infinies
    var relayoutLock = false;

    // Fonction pour lier les axes X de tous les graphiques
    function linkCharts(chartId) {{
      const chartDiv = document.getElementById(chartId);
      if (!chartDiv) return;
      chartDiv.on('plotly_relayout', (eventData) => {{
        if (relayoutLock) return;
        // Si l'événement contient des changements sur l'axe X, on les propage
        const update = {{}};
        if (eventData['xaxis.range[0]'] !== undefined && eventData['xaxis.range[1]'] !== undefined) {{
          update['xaxis.range'] = [ eventData['xaxis.range[0]'], eventData['xaxis.range[1]'] ];
        }}
        if (eventData['xaxis.autorange'] === true) {{
          update['xaxis.autorange'] = true;
        }}
        if (Object.keys(update).length > 0) {{
          relayoutLock = true;
          chartIds.forEach(otherId => {{
            if (otherId !== chartId) {{
              Plotly.relayout(otherId, update);
            }}
          }});
          setTimeout(() => {{ relayoutLock = false; }}, 50);
        }}
      }});
    }}
    
    function parseTimestamp(ts) {{
      return new Date(ts);
    }}

    // FRAME & LPAR dropdowns
    const frameSelect = document.getElementById("frame_select");
    const lparSelect = document.getElementById("lpar_select");
    const lparSearch = document.getElementById("lpar_search");
    const lparDataList = document.getElementById("lpar_datalist");
    const allLpars = Object.keys(frameMap).sort();

    const frames = Array.from(new Set(Object.values(frameMap))).sort();
    for (const fr of frames) {{
      const opt = document.createElement("option");
      opt.value = fr;
      opt.text = fr;
      frameSelect.appendChild(opt);
    }}

    function populateLpars() {{
      const frameVal = frameSelect.value;
      lparSelect.innerHTML = "";
      Object.keys(frameMap).sort().forEach(nm => {{
        if (frameMap[nm] === frameVal) {{
          const opt = document.createElement("option");
          opt.value = nm;
          opt.text = nm;
          lparSelect.appendChild(opt);
        }}
      }});
    }}

    populateLpars();

    // Populate datalist for LPAR search
    allLpars.forEach(nm => {{
      const opt = document.createElement('option');
      opt.value = nm;
      lparDataList.appendChild(opt);
    }});

    lparSearch.addEventListener('change', () => {{
      const val = lparSearch.value;
      if (allLpars.includes(val)) {{
        frameSelect.value = frameMap[val] || frameSelect.value;
        populateLpars();
        lparSelect.value = val;
        renderCharts();
      }}
    }});

    function getFilteredDocs() {{
      const sel = lparSelect.value;
      let docs = lparDataMap[sel] || [];
      const startVal = document.getElementById("start_date").value;
      const endVal   = document.getElementById("end_date").value;
      if (startVal) {{
        const startDate = new Date(startVal);
        docs = docs.filter(d => parseTimestamp(d["@timestamp"]) >= startDate);
      }}
      if (endVal) {{
        const endDate = new Date(endVal);
        endDate.setHours(23,59,59,999);
        docs = docs.filter(d => parseTimestamp(d["@timestamp"]) <= endDate);
      }}
      docs.sort((a,b) => parseTimestamp(a["@timestamp"]) - parseTimestamp(b["@timestamp"]));
      return docs;
    }}

    function getFilteredTopDocs() {{
      const sel = lparSelect.value;
      let tdocs = topDataMap[sel] || [];
      const startVal = document.getElementById("start_date").value;
      const endVal   = document.getElementById("end_date").value;
      if (startVal) {{
        const startDate = new Date(startVal);
        tdocs = tdocs.filter(d => parseTimestamp(d["@timestamp"]) >= startDate);
      }}
      if (endVal) {{
        const endDate = new Date(endVal);
        endDate.setHours(23,59,59,999);
        tdocs = tdocs.filter(d => parseTimestamp(d["@timestamp"]) <= endDate);
      }}
      tdocs.sort((a,b) => parseTimestamp(a["@timestamp"]) - parseTimestamp(b["@timestamp"]));
      return tdocs;
    }}

    function getFilteredLpmDocs() {{
      const sel = lparSelect.value;
      let lpmdocs = lpmDataMap[sel] || [];
      const startVal = document.getElementById("start_date").value;
      const endVal   = document.getElementById("end_date").value;
      if (startVal) {{
        const startDate = new Date(startVal);
        lpmdocs = lpmdocs.filter(d => parseTimestamp(d["@timestamp"]) >= startDate);
      }}
      if (endVal) {{
        const endDate = new Date(endVal);
        endDate.setHours(23,59,59,999);
        lpmdocs = lpmdocs.filter(d => parseTimestamp(d["@timestamp"]) <= endDate);
      }}
      lpmdocs.sort((a,b) => parseTimestamp(a["@timestamp"]) - parseTimestamp(b["@timestamp"]));
      return lpmdocs;
    }}

    function updateChartLayout() {{
      const cols = parseInt(document.getElementById("chartsPerRow").value, 10) || 3;
      const container = document.getElementById("chartsContainer");
      if (container) {{
        container.style.gridTemplateColumns = 'repeat(' + cols + ', minmax(0, 1fr))';
      }}
    }}

    function renderCharts() {{
      updateChartLayout();
      const ctx = document.getElementById('analytics-context');
      if (ctx) {{
        ctx.textContent = (frameSelect.value || 'Frame') + ' · ' + (lparSelect.value || 'LPAR');
      }}
      const docs = getFilteredDocs();
      if (!docs.length) {{
        chartIds.forEach(id => {{
          document.getElementById(id).innerHTML = "<p>No data</p>";
        }});
        return;
      }}
      const times = docs.map(d => parseTimestamp(d["@timestamp"]));
      const xRange = [times[0], times[times.length - 1]];
      
      // 1) CPU usage
      const userVals = docs.map(d => d.cpu_all ? d.cpu_all["User%"] : null);
      const sysVals  = docs.map(d => d.cpu_all ? d.cpu_all["Sys%"]  : null);
      const idleVals = docs.map(d => d.cpu_all ? d.cpu_all["Idle%"] : null);
      const waitVals = docs.map(d => d.cpu_all ? d.cpu_all["Wait%"] : null);
      Plotly.newPlot('cpu_usage_chart', [
        {{ x: times, y: userVals, mode: 'lines', name: 'User%', stackgroup: 'one',line: {{ color: themeColor('--chart-1') }} ,connectgaps: false , stackgaps: false}},
        {{ x: times, y: sysVals,  mode: 'lines', name: 'Sys%',  stackgroup: 'one', line: {{ color: themeColor('--danger') }} ,connectgaps: false , stackgaps: false}},
        {{ x: times, y: waitVals, mode: 'lines', name: 'Wait%', stackgroup: 'one',line: {{ color: themeColor('--warning') }} ,connectgaps: false , stackgaps: false}},
        {{ x: times, y: idleVals, mode: 'lines', name: 'Idle%', stackgroup: 'one', line: {{ color: themeColor('--success') }} ,connectgaps: false , stackgaps: false}}
      ], {{
        title: 'CPU Usage (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Percentage' }}
      }})
      // --- MODIFICATION EXPLICITE : PARTIE 3 ---
      // C'est ici que nous connectons la fonction à chaque graphique.
      // Le '.then()' garantit que la liaison se fait après la création du graphique.
      .then(gd => linkCharts('cpu_usage_chart'));

      // NEW: Average Use of Logical CPU Core Threads - POWER=SMT (Stacked Bar Chart)
      let cpuAgg = {{}};
      docs.forEach(d => {{
         if(d.cpu_use) {{
              for (let cpu in d.cpu_use) {{
                   if (!(cpu in cpuAgg)) {{
                        cpuAgg[cpu] = {{ user_sum: 0, sys_sum: 0, count: 0 }};
                   }}
                   cpuAgg[cpu].user_sum += d.cpu_use[cpu].user;
                   cpuAgg[cpu].sys_sum += d.cpu_use[cpu].sys;
                   cpuAgg[cpu].count++;
              }}
         }}
      }});
      let cpuLabels = [];
      let userAverages = [];
      let sysAverages = [];
      for (let cpu in cpuAgg) {{
         let count = cpuAgg[cpu].count;
         let avgUser = cpuAgg[cpu].user_sum / count;
         let avgSys = cpuAgg[cpu].sys_sum / count;
         if ((avgUser + avgSys) > 0.05) {{
              cpuLabels.push("CPU" + cpu);
              userAverages.push(avgUser);
              sysAverages.push(avgSys);
         }}
      }}
      let combined = cpuLabels.map((label, i) => ({{ cpu: label, user: userAverages[i], sys: sysAverages[i] }}));
      combined.sort((a, b) => {{
         return parseInt(a.cpu.replace("CPU","")) - parseInt(b.cpu.replace("CPU",""));
      }});
      cpuLabels = combined.map(item => item.cpu);
      userAverages = combined.map(item => item.user);
      sysAverages = combined.map(item => item.sys);
      var traceUser = {{
         x: cpuLabels,
         y: userAverages,
         name: 'User%',
         type: 'bar'
      }};
      var traceSys = {{
         x: cpuLabels,
         y: sysAverages,
         name: 'System%',
         type: 'bar'
      }};
      Plotly.newPlot('cpu_use_chart', [traceUser, traceSys], {{
         title: 'Average Use of LCPU Core Threads - POWER=SMT',
         barmode: 'stack',
         xaxis: {{ title: 'CPU Core' }},
         yaxis: {{ title: '% Usage' }}
      }})
      // --- MODIFICATION EXPLICITE : PARTIE 3 (répétée pour chaque graphique) ---
      .then(gd => linkCharts('cpu_use_chart'));

      // 2) LPAR usage
      const physVals = docs.map(d => d.lpar ? d.lpar["PhysicalCPU"] : null);
      const virtVals = docs.map(d => d.lpar ? d.lpar["VirtualCPUs"] : null);
      const entVals  = docs.map(d => d.lpar ? d.lpar["Entitled"]    : null);
      Plotly.newPlot('lpar_usage_chart', [
        {{ x: times, y: physVals, mode: 'lines', fill: 'tozeroy', fillcolor: 'rgba(0, 123, 255, 0.1)', name: 'PhysicalCPU' , connectgaps: false }},
        {{ x: times, y: virtVals, mode: 'lines', name: 'VirtualCPUs' , connectgaps: false }},
        {{ x: times, y: entVals,  mode: 'lines', name: 'Entitled' , connectgaps: false }}
      ], {{
        title: 'LPAR Usage (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'CPU Count', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('lpar_usage_chart'));
      
      // NEW: Pool CPUs & Pool Idle
      const poolCPUsVals = docs.map(d => d.lpar ? d.lpar["PoolCPUs"] : null);
      const poolIdleVals = docs.map(d => d.lpar ? d.lpar["PoolIdle"] : null);
      Plotly.newPlot('pool_usage_chart', [
        {{ x: times, y: poolCPUsVals, mode: 'lines', fill: 'tozeroy', fillcolor: 'rgba(0, 123, 255, 0.1)',name: 'PoolCPUs' , connectgaps: false }},
        {{ x: times, y: poolIdleVals, mode: 'lines', fill: 'tozeroy',line: {{ color: themeColor('--success') }},fillcolor: themeRgba('--success', 0.45),name: 'PoolIdle' , connectgaps: false }}
      ], {{
        title: {{ text: `Pool CPUs & Pool Idle (${{lparSelect.value}})<br><span style="font-size:12px">PoolIdle=0 --> allow_perf_collection = 0</span>`, x: 0.5, xanchor:'center'}},
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Count/Percentage' }}
      }}).then(gd => linkCharts('pool_usage_chart'));


      // 3) Runnable
      const runVals = docs.map(d => d.proc ? d.proc["Runnable"] : null);
      Plotly.newPlot('runnable_chart', [
        {{ x: times, y: runVals, mode: 'lines', fill: 'tozeroy', name: 'Runnable' , connectgaps: false}}
      ], {{
        title: 'Run Queue (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Count', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('runnable_chart'));

      // 4) Syscall/Read/Write
      const syscallVals = docs.map(d => d.proc ? d.proc["Syscall"] : null);
      const readVals    = docs.map(d => d.proc ? d.proc["Read"]   : null);
      const writeVals   = docs.map(d => d.proc ? d.proc["Write"]   : null);
      Plotly.newPlot('syscall_chart', [
        {{ x: times, y: syscallVals, mode: 'lines', name: 'Syscall' , connectgaps: false}},
        {{ x: times, y: readVals,    mode: 'lines', name: 'Read' , connectgaps: false }},
        {{ x: times, y: writeVals,   mode: 'lines', name: 'Write' , connectgaps: false }}
      ], {{
        title: 'Syscall / Read / Write',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Calls/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('syscall_chart'));

      // 5) pswitch
      const pswVals = docs.map(d => d.proc ? d.proc["pswitch"] : null);
      Plotly.newPlot('pswitch_chart', [
        {{ x: times, y: pswVals, mode: 'lines', fill: 'tozeroy', name: 'pswitch'  , connectgaps: false }}
      ], {{
        title: 'Process Switches',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Switches/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('pswitch_chart'));

      // 6) fork+exec
      const forkVals = docs.map(d => d.proc ? d.proc["fork"] : null);
      const execVals = docs.map(d => d.proc ? d.proc["exec"] : null);
      Plotly.newPlot('fork_exec_chart', [
        {{ x: times, y: forkVals, mode: 'lines', name: 'fork' , connectgaps: false}},
        {{ x: times, y: execVals, mode: 'lines', name: 'exec' , connectgaps: false }}
      ], {{
        title: 'fork() & exec()',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Calls/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('fork_exec_chart'));

      // NEW: InterProcess Comms - Semaphores/s & Msg Queues send/s
      const semVals = docs.map(d => d.proc ? d.proc["sem"] : null);
      const msgVals = docs.map(d => d.proc ? d.proc["msg"] : null);
      Plotly.newPlot('sem_msg_chart', [
        {{ x: times, y: semVals, mode: 'lines', name: 'sem' , connectgaps: false}},
        {{ x: times, y: msgVals, mode: 'lines', name: 'msg' , connectgaps: false }}
      ], {{
        title: {{ text: `InterProcess Comms (${{lparSelect.value}})<br><span style="font-size:12px">Semaphores/s & Msg Queues send/s</span>`, x: 0.5, xanchor:'center'}},
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Calls/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('sem_msg_chart'));

      // 7) File I/O
      const readchVals  = docs.map(d => d.file_io ? (d.file_io["readch"]  || 0) : null);
      const writechVals = docs.map(d => d.file_io ? (d.file_io["writech"] || 0) : null);
      const negWrite    = writechVals.map(v => -Math.abs(v));
      Plotly.newPlot('fileio_chart', [
        {{ x: times, y: readchVals, mode: 'lines', name: 'readch',  stackgroup: 'one' , connectgaps: false }},
        {{ x: times, y: negWrite,   mode: 'lines', name: 'writech', stackgroup: 'two' , connectgaps: false }}
      ], {{
        title: 'File I/O: readch & writech',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Bytes', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('fileio_chart'));

      // NEW: TOPSUM Bubble Chart for TOP data (aggregated by Command)
      const topdocs = getFilteredTopDocs();
      if (!topdocs.length) {{
         document.getElementById("top_bubble_chart").innerHTML = "<p>No TOP data</p>";
      }} else {{
         let bubbleData = {{}};
         topdocs.forEach(function(td) {{
              let cmd = td["Command"] || "unknown";
              let cpuVal = td["%CPU"] || 0;
              let charioVal = td["CharIO"] || 0;
              let memVal = td["Memory"] || 0;
              if (!(cmd in bubbleData)) {{
                  bubbleData[cmd] = {{ cpu: 0, chario: 0, mem: 0 }};
              }}
              bubbleData[cmd].cpu += cpuVal;
              bubbleData[cmd].chario += charioVal;
              if (memVal > bubbleData[cmd].mem) {{
                  bubbleData[cmd].mem = memVal;
              }}
         }});
         let bubbleArray = [];
         for (let cmd in bubbleData) {{
              bubbleArray.push({{ command: cmd, cpu: bubbleData[cmd].cpu, chario: bubbleData[cmd].chario / 1024, mem: bubbleData[cmd].mem }});
         }}
         bubbleArray.sort((a, b) => b.cpu - a.cpu);
         bubbleArray = bubbleArray.slice(0, 20);
         let maxSize = Math.max(...bubbleArray.map(item => item.mem));
         let bubbleTraces = bubbleArray.map(item => {{
              return {{
                x: [item.cpu],
                y: [item.chario],
                text: [item.command],
                name: item.command,
                mode: 'markers',
                marker: {{
                  size: [item.mem],
                  sizemode: 'area',
                  sizeref: 2.0 * maxSize / (100**2),
                  sizemin: 4
                }},
                hovertemplate: 'Command: %{{text}}<br>CPU: %{{x:.1f}}<br>Char I/O: %{{y:.1f}} KB<br>Memory: %{{marker.size}} KB<extra></extra>'
              }};
         }});
         Plotly.newPlot('top_bubble_chart', bubbleTraces, {{
              title: {{ text: `Top 20 Processes by CPU Correlation  (${{lparSelect.value}})<br><span style="font-size:12px">(Total CPU Seconds, Character I/O, Max Memory Size)</span>`, x: 0.5, xanchor:'center'}},
              xaxis: {{ title: 'CPU seconds in Total' }},
              yaxis: {{ title: 'Character I/O in Total (KB)' }},
              legend: {{
                  x: 1.05,
                  y: 1,
                  orientation: "v",
                  font: {{ size: 10 }}
              }},
              margin: {{ t: 50, r: 150 }}
         }}).then(gd => linkCharts('top_bubble_chart'));
      }}

      // 8) TOP CPU - modified to align with ksh logic (by Command)
      if (!topdocs.length) {{
        document.getElementById("top_cpu_chart").innerHTML = "<p>No TOP data</p>";
      }} else {{
        // Group by timestamp with keys as Command
        let dataByTimestamp = {{}};
        let commandsSet = new Set();
        for (const td of topdocs) {{
          const tsStr = td["@timestamp"];
          const time = parseTimestamp(tsStr);
          const cpuVal = td["%CPU"] || 0;
          const cmd = td["Command"] || "unknown";
          commandsSet.add(cmd);
          if (!(tsStr in dataByTimestamp)) {{
            dataByTimestamp[tsStr] = {{ time: time, commands: {{}} }};
          }}
          dataByTimestamp[tsStr].commands[cmd] = (dataByTimestamp[tsStr].commands[cmd] || 0) + cpuVal;
        }}
        let sortedTimestampsKeys = Object.keys(dataByTimestamp).sort((a, b) => {{
          return dataByTimestamp[a].time - dataByTimestamp[b].time;
        }});
        let sortedCommands = Array.from(commandsSet).sort();
        let traces = [];
        for (const command of sortedCommands) {{
          let xArr = [];
          let yArr = [];
          for (const tsKey of sortedTimestampsKeys) {{
            xArr.push(dataByTimestamp[tsKey].time);
            yArr.push(dataByTimestamp[tsKey].commands[command] || 0);
          }}
          traces.push({{
            x: xArr,
            y: yArr,
            name: command,
            mode: 'lines'
          }});
        }}
        Plotly.newPlot('top_cpu_chart', traces, {{
          title: 'TOP Commands by %CPU (' + lparSelect.value + ')',
          xaxis: {{ title: 'Time' }},
          yaxis: {{ title: '%CPU (per process)', rangemode: 'tozero' }}
        }}).then(gd => linkCharts('top_cpu_chart'));
      }}

      // NEW: TOP Commands by %CPU (Stacked) chart
      if (topdocs.length) {{
         let dataByTimestampStacked = {{}};
         let commandsSetStacked = new Set();
         for (const td of topdocs) {{
            const tsStr = td["@timestamp"];
            const time = parseTimestamp(tsStr);
            const cpuVal = td["%CPU"] || 0;
            const cmd = td["Command"] || "unknown";
            commandsSetStacked.add(cmd);
            if (!(tsStr in dataByTimestampStacked)) {{
                dataByTimestampStacked[tsStr] = {{ time: time, commands: {{}} }};
            }}
            dataByTimestampStacked[tsStr].commands[cmd] = (dataByTimestampStacked[tsStr].commands[cmd] || 0) + cpuVal;
         }}
         let sortedTimestampsKeysStacked = Object.keys(dataByTimestampStacked).sort((a, b) => {{
             return dataByTimestampStacked[a].time - dataByTimestampStacked[b].time;
         }});
         let sortedCommandsStacked = Array.from(commandsSetStacked).sort();
         let stackedTraces = [];
         let counter = 0;
         for (const command of sortedCommandsStacked) {{
             let xArr = [];
             let yArr = [];
             for (const tsKey of sortedTimestampsKeysStacked) {{
                 xArr.push(dataByTimestampStacked[tsKey].time);
                 yArr.push(dataByTimestampStacked[tsKey].commands[command] || 0);
             }}
             let fillMode = (counter === 0) ? 'tozeroy' : 'tonexty';
             stackedTraces.push({{
                 x: xArr,
                 y: yArr,
                 name: command,
                 mode: 'lines',
                 stackgroup: 'commands_stacked',
                 fill: fillMode
             }});
             counter++;
         }}
         Plotly.newPlot('top_cpu_stacked_chart', stackedTraces, {{
             title: 'TOP Commands by %CPU (Stacked) (' + lparSelect.value + ')',
             xaxis: {{ title: 'Time' }},
             yaxis: {{ title: '%CPU (per command)', rangemode: 'tozero' }}
         }}).then(gd => linkCharts('top_cpu_stacked_chart'));

      }} else {{
         document.getElementById("top_cpu_stacked_chart").innerHTML = "<p>No TOP data</p>";
      }}

      // NEW: Top 20 Process PIDs by CPU Correlation (Bubble)
      if (!topdocs.length) {{
         document.getElementById("top_pid_bubble_chart").innerHTML = "<p>No TOP data</p>";
      }} else {{
         let bubbleDataPid = {{}};
         topdocs.forEach(function(td) {{
              let pid = td["PID"] || "unknown";
              let cpuVal = td["%CPU"] || 0;
              let charioVal = td["CharIO"] || 0;
              let memVal = td["Memory"] || 0;
              if (!(pid in bubbleDataPid)) {{
                  bubbleDataPid[pid] = {{ cpu: 0, chario: 0, mem: 0 }};
              }}
              bubbleDataPid[pid].cpu += cpuVal;
              bubbleDataPid[pid].chario += charioVal;
              if (memVal > bubbleDataPid[pid].mem) {{
                  bubbleDataPid[pid].mem = memVal;
              }}
         }});
         let bubbleArrayPid = [];
         for (let p in bubbleDataPid) {{
              bubbleArrayPid.push({{ pid: p, cpu: bubbleDataPid[p].cpu, chario: bubbleDataPid[p].chario / 1024, mem: bubbleDataPid[p].mem }});
         }}
         bubbleArrayPid.sort((a,b) => b.cpu - a.cpu);
         bubbleArrayPid = bubbleArrayPid.slice(0,20);
         let maxSizePid = Math.max(...bubbleArrayPid.map(item => item.mem));
         let bubbleTracesPid = bubbleArrayPid.map(item => {{
              return {{
                x: [item.cpu],
                y: [item.chario],
                text: [item.pid],
                name: item.pid,
                mode: 'markers',
                marker: {{
                  size: [item.mem],
                  sizemode: 'area',
                  sizeref: 2.0 * maxSizePid / (100**2),
                  sizemin: 4
                }},
                hovertemplate: 'PID: %{{text}}<br>CPU: %{{x:.1f}}<br>Char I/O: %{{y:.1f}} KB<br>Memory: %{{marker.size}} KB<extra></extra>'
              }};
         }});
         Plotly.newPlot('top_pid_bubble_chart', bubbleTracesPid, {{
              title: {{ text: `Top 20 Process PIDs by CPU Correlation  (${{lparSelect.value}})<br><span style="font-size:12px">(Total CPU Seconds, Character I/O, Max Memory Size)</span>`, x: 0.5, xanchor:'center'}},
              xaxis: {{ title: 'CPU seconds in Total' }},
              yaxis: {{ title: 'Character I/O in Total (KB)' }},
              legend: {{ x: 1.05, y: 1, orientation: 'v', font: {{ size: 10 }} }},
              margin: {{ t: 50, r: 150 }}
         }}).then(gd => linkCharts('top_pid_bubble_chart'));
      }}

      // NEW: Top 20 Process PIDs by CPU (Unstacked)

      // NEW: Top 20 Process PIDs by CPU (Unstacked)
      let totalByPid = {{}};
      for (const td of topdocs) {{
          const pid = td["PID"] || "unknown";
          const cpu = td["%CPU"] || 0;
          totalByPid[pid] = (totalByPid[pid] || 0) + cpu;
      }}
      let topPIDs = Object.keys(totalByPid)
                          .sort((a, b) => totalByPid[b] - totalByPid[a])
                          .slice(0, 20);
      let dataByTimestampPID = {{}};
      for (const td of topdocs) {{
          const tsStr = td["@timestamp"];
          const time = parseTimestamp(tsStr);
          const cpu = td["%CPU"] || 0;
          const pid = td["PID"] || "unknown";
          if (!topPIDs.includes(pid)) continue;
          if (!(tsStr in dataByTimestampPID)) {{
              dataByTimestampPID[tsStr] = {{ time: time, pids: {{}} }};
          }}
          dataByTimestampPID[tsStr].pids[pid] = (dataByTimestampPID[tsStr].pids[pid] || 0) + cpu;
      }}
      let sortedTimestampsPID = Object.keys(dataByTimestampPID).sort((a, b) => {{
          return dataByTimestampPID[a].time - dataByTimestampPID[b].time;
      }});
      let pidTraces = [];
      for (const pid of topPIDs) {{
         let xArr = [];
         let yArr = [];
         for (const ts of sortedTimestampsPID) {{
             xArr.push(dataByTimestampPID[ts].time);
             yArr.push(dataByTimestampPID[ts].pids[pid] || 0);
         }}
         pidTraces.push({{
              x: xArr,
              y: yArr,
              name: pid,
              mode: 'lines'
         }});
      }}
      Plotly.newPlot('top_pid_chart', pidTraces, {{
          title: 'Top 20 Process PIDs by CPU (' + lparSelect.value + ')',
          xaxis: {{ title: 'Time' }},
          yaxis: {{ title: '%CPU (per process)', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('top_pid_chart'));

      // NEW: Top 20 Process PIDs by CPU (Stacked)
      let stackedPidTraces = [];
      let counterPid = 0;
      for (const pid of topPIDs) {{
         let xArr = [];
         let yArr = [];
         for (const ts of sortedTimestampsPID) {{
             xArr.push(dataByTimestampPID[ts].time);
             yArr.push(dataByTimestampPID[ts].pids[pid] || 0);
         }}
         let fillMode = (counterPid === 0) ? 'tozeroy' : 'tonexty';
         stackedPidTraces.push({{
             x: xArr,
             y: yArr,
             name: pid,
             mode: 'lines',
             stackgroup: 'pid_stacked',
             fill: fillMode
         }});
         counterPid++;
      }}
      Plotly.newPlot('top_pid_stacked_chart', stackedPidTraces, {{
          title: 'Top 20 Process PIDs by CPU (Stacked) (' + lparSelect.value + ')',
          xaxis: {{ title: 'Time' }},
          yaxis: {{ title: '%CPU (per process)', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('top_pid_stacked_chart'));

      // NEW: FS Cache Memory Use (numperm) Percentage chart
      const numpermVals = docs.map(d => d.memuse ? d.memuse["numperm"] : null);
      const minpermVals = docs.map(d => d.memuse ? d.memuse["minperm"] : null);
      const maxpermVals = docs.map(d => d.memuse ? d.memuse["maxperm"] : null);
      Plotly.newPlot('fs_cache_chart', [
        {{ x: times, y: numpermVals, mode: 'lines', name: 'numperm' , connectgaps: false }},
        {{ x: times, y: minpermVals, mode: 'lines', name: 'minperm' , connectgaps: false}},
        {{ x: times, y: maxpermVals, mode: 'lines', name: 'maxperm' , connectgaps: false}}
      ], {{
        title: 'FS Cache Memory Use (numperm) Percentage (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Percentage', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('fs_cache_chart'));

      // 9) MEMNEW
      const memSystem = docs.map(d => d.memnew ? d.memnew["System%"]  : null);
      const memFScache = docs.map(d => d.memnew ? d.memnew["FScache%"] : null);
      const memProcess = docs.map(d => d.memnew ? d.memnew["Process%"] : null);
      const memFree = docs.map(d => d.memnew ? d.memnew["Free%"] : null);
      Plotly.newPlot('memnew_chart', [
        {{ x: times, y: memProcess, mode: 'lines', name: 'Process%', stackgroup: 'one', line: {{ color: themeColor('--chart-1') }} , connectgaps: false }},
        {{ x: times, y: memFScache, mode: 'lines', name: 'FScache%', stackgroup: 'one', line: {{ color: themeColor('--danger') }} , connectgaps: false}},
        {{ x: times, y: memSystem, mode: 'lines', name: 'System%',  stackgroup: 'one', line: {{ color: themeColor('--warning') }} , connectgaps: false}},
        {{ x: times, y: memFree,    mode: 'lines', name: 'Free%',    stackgroup: 'one', line: {{ color: themeColor('--success') }} , connectgaps: false }}
      ], {{
        title: 'Memory Usage (MEMNEW) (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time' }},
        yaxis: {{ title: 'Percentage', range: [0, 100] }}
      }}).then(gd => linkCharts('memnew_chart'));

      // NEW: MEM MB chart
      const realTotal = docs.map(d => d.mem_mb ? d.mem_mb["Real_Total_MB"] : 0);
      const virtTotal = docs.map(d => d.mem_mb ? d.mem_mb["Virtual_Total_MB"] : 0);
      const realUsedMB = docs.map(d => d.mem_mb ? d.mem_mb["Real_Used_MB"] : 0);
      const virtUsedMB = docs.map(d => d.mem_mb ? d.mem_mb["Virtual_Used_MB"] : 0);
      Plotly.newPlot('mem_mb_chart', [
        {{ x: times, y: realTotal, mode: 'lines', name: 'Real Total (MB)' }},
        {{ x: times, y: virtTotal, mode: 'lines', name: 'Virtual Total (MB)' }},
        {{ x: times, y: realUsedMB, mode: 'lines', fill: 'tozeroy', name: 'Real Used (MB)' }},
        {{ x: times, y: virtUsedMB, mode: 'lines', fill: 'tozeroy', name: 'Virtual Used (MB)' }}
      ], {{
        title: 'Memory Usage (MB) (MEM) (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Memory (MB)' }}
      }}).then(gd => linkCharts('mem_mb_chart'));

      // 10) MEM used%
      const realUsed = docs.map(d => d.mem ? d.mem["Real_Used%"]    : 0);
      const virtUsed = docs.map(d => d.mem ? d.mem["Virtual_Used%"] : 0);
      Plotly.newPlot('memused_chart', [
        {{ x: times, y: realUsed, mode: 'lines', fill: 'tozeroy', fillcolor: 'rgba(0, 123, 255, 0.1)', name: 'Real_Used%' }},
        {{ x: times, y: virtUsed, mode: 'lines', name: 'Virtual_Used%' }}
      ], {{
        title: 'Memory Used% (MEM) (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time' }},
        yaxis: {{ title: 'Used %', range: [0, 100] }}
      }}).then(gd => linkCharts('memused_chart'));

      // 11) Swap-in
      const swapinVals = docs.map(d => d.proc ? d.proc["Swap-in"] : 0);
      Plotly.newPlot('swapin_chart', [
        {{ x: times, y: swapinVals, mode: 'lines', fill: 'tozeroy', name: 'Swap-in' }}
      ], {{
        title: 'Swap-in (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time' }},
        yaxis: {{ title: 'Occurrences/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('swapin_chart'));

      // NEW: All Paging per second chart (from PAGE lines)
      const pginVals = docs.map(d => d.page ? d.page["pgin"] : 0);
      const pgoutVals = docs.map(d => d.page ? -Math.abs(d.page["pgout"]) : 0);
      const pgsinVals = docs.map(d => d.page ? d.page["pgsin"] : 0);
      const pgsoutVals = docs.map(d => d.page ? -Math.abs(d.page["pgsout"]) : 0);
      Plotly.newPlot('paging_chart', [
        {{ x: times, y: pginVals, mode: 'lines', name: 'pgin' }},
        {{ x: times, y: pgoutVals, mode: 'lines', name: 'pgout' }},
        {{ x: times, y: pgsinVals, mode: 'lines', name: 'pgsin' }},
        {{ x: times, y: pgsoutVals, mode: 'lines', name: 'pgsout' }}
      ], {{
        title: 'All Paging per second (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Paging/sec', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('paging_chart'));

      // 12) NET usage => read/write
      const netTracesByColumn = {{}};
      const netColumnsSet = new Set();
      docs.forEach(d => {{
        if (d.net) {{
          for (const colName in d.net) {{
            netColumnsSet.add(colName);
          }}
        }}
      }});
      netColumnsSet.forEach(colName => {{
        netTracesByColumn[colName] = {{ x: [], y: [] }};
      }});
      docs.forEach(d => {{
        const t = parseTimestamp(d["@timestamp"]);
        if (d.net) {{
          for (const colName of netColumnsSet) {{
            const val = d.net[colName] !== undefined ? d.net[colName] : 0;
            netTracesByColumn[colName].x.push(t);
            netTracesByColumn[colName].y.push(val);
          }}
        }} else {{
          for (const colName of netColumnsSet) {{
            netTracesByColumn[colName].x.push(t);
            netTracesByColumn[colName].y.push(0);
          }}
        }}
      }});
      const netTraces = [];
      for (const [colName, arrObj] of Object.entries(netTracesByColumn)) {{
        const dashIndex = colName.indexOf('-');
        let iface = colName;
        let direction = "";
        if (dashIndex > 0) {{
          iface = colName.substring(0, dashIndex);
          const afterDash = colName.substring(dashIndex+1);
          if (afterDash.startsWith("read")) {{
            direction = "read";
          }} else if (afterDash.startsWith("write")) {{
            direction = "write";
          }} else {{
            direction = afterDash;
          }}
        }}
        const traceName = iface + " " + direction;
        const clonedY = arrObj.y.map(v => v);
        if (direction === 'write') {{
          for (let i = 0; i < clonedY.length; i++) {{
            clonedY[i] = -Math.abs(clonedY[i]);
          }}
        }}
        netTraces.push({{
          x: arrObj.x,
          y: clonedY,
          mode: 'lines',
          name: traceName
        }});
      }}
      Plotly.newPlot('net_chart', netTraces, {{
        title: 'Network Read/Write (KB/s) (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time' }},
        yaxis: {{ title: 'KB/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('net_chart'));

      // New: NET Stacked chart (separate stack groups for read and write)
      const netStackedTraces = [];
      let netReadIndex = 0;
      let netWriteIndex = 0;
      for (const [colName, arrObj] of Object.entries(netTracesByColumn)) {{
        const dashIndex = colName.indexOf('-');
        let iface = colName;
        let direction = "";
        if (dashIndex > 0) {{
          iface = colName.substring(0, dashIndex);
          const afterDash = colName.substring(dashIndex+1);
          if (afterDash.startsWith("read")) {{
            direction = "read";
          }} else if (afterDash.startsWith("write")) {{
            direction = "write";
          }} else {{
            direction = afterDash;
          }}
        }}
        const traceName = iface + " " + direction;
        const clonedY = arrObj.y.map(v => v);
        if (direction === 'write') {{
          for (let i = 0; i < clonedY.length; i++) {{
            clonedY[i] = -Math.abs(clonedY[i]);
          }}
        }}
        let fillMode;
        if (direction === 'read') {{
          fillMode = (netReadIndex === 0) ? 'tozeroy' : 'tonexty';
          netStackedTraces.push({{
            x: arrObj.x,
            y: clonedY,
            mode: 'lines',
            name: traceName,
            stackgroup: 'net_stacked_read',
            fill: fillMode
          }});
          netReadIndex++;
        }} else if (direction === 'write') {{
          fillMode = (netWriteIndex === 0) ? 'tozeroy' : 'tonexty';
          netStackedTraces.push({{
            x: arrObj.x,
            y: clonedY,
            mode: 'lines',
            name: traceName,
            stackgroup: 'net_stacked_write',
            fill: fillMode
          }});
          netWriteIndex++;
        }} else {{
          fillMode = 'tozeroy';
          netStackedTraces.push({{
            x: arrObj.x,
            y: clonedY,
            mode: 'lines',
            name: traceName,
            stackgroup: 'net_stacked',
            fill: fillMode
          }});
        }}
      }}
      Plotly.newPlot('net_stacked_chart', netStackedTraces, {{
        title: 'Network Read/Write - Stacked (KB/s) (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time' }},
        yaxis: {{ title: 'KB/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('net_stacked_chart'));

      // 13) NETPACKET chart
      const netpacketTracesByColumn = {{}};
      const netpacketColumnsSet = new Set();
      docs.forEach(d => {{
        if (d.netpacket) {{
          for (const colName in d.netpacket) {{
            netpacketColumnsSet.add(colName);
          }}
        }}
      }});
      netpacketColumnsSet.forEach(colName => {{
        netpacketTracesByColumn[colName] = {{ x: [], y: [] }};
      }});
      docs.forEach(d => {{
        const t = parseTimestamp(d["@timestamp"]);
        if (d.netpacket) {{
          for (const colName of netpacketColumnsSet) {{
            const val = d.netpacket[colName] !== undefined ? d.netpacket[colName] : 0;
            netpacketTracesByColumn[colName].x.push(t);
            netpacketTracesByColumn[colName].y.push(val);
          }}
        }} else {{
          for (const colName of netpacketColumnsSet) {{
            netpacketTracesByColumn[colName].x.push(t);
            netpacketTracesByColumn[colName].y.push(0);
          }}
        }}
      }});
      const netpacketTraces = [];
      for (const [colName, arrObj] of Object.entries(netpacketTracesByColumn)) {{
        const dashIndex = colName.indexOf('-');
        let iface = colName;
        let direction = "";
        if (dashIndex > 0) {{
          iface = colName.substring(0, dashIndex);
          const afterDash = colName.substring(dashIndex+1);
          if (afterDash.startsWith("read")) {{
            direction = "reads";
          }} else if (afterDash.startsWith("write")) {{
            direction = "writes";
          }} else {{
            direction = afterDash;
          }}
        }}
        const traceName = iface + " " + direction;
        const clonedY = arrObj.y.map(v => v);
        if (direction === 'writes') {{
          for (let i = 0; i < clonedY.length; i++) {{
            clonedY[i] = -Math.abs(clonedY[i]);
          }}
        }}
        netpacketTraces.push({{
          x: arrObj.x,
          y: clonedY,
          mode: 'lines',
          name: traceName
        }});
      }}
      Plotly.newPlot('netpacket_chart', netpacketTraces, {{
        title: 'Network Packets Read/Writes/s (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time' }},
        yaxis: {{ title: 'Packets/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('netpacket_chart'));

      // 14) NETSIZE chart
      const netsizeTracesByColumn = {{}};
      const netsizeColumnsSet = new Set();
      docs.forEach(d => {{
        if (d.netsize) {{
          for (const colName in d.netsize) {{
            netsizeColumnsSet.add(colName);
          }}
        }}
      }});
      netsizeColumnsSet.forEach(colName => {{
        netsizeTracesByColumn[colName] = {{ x: [], y: [] }};
      }});
      docs.forEach(d => {{
        const t = parseTimestamp(d["@timestamp"]);
        if (d.netsize) {{
          for (const colName of netsizeColumnsSet) {{
            const val = d.netsize[colName] !== undefined ? d.netsize[colName] : 0;
            netsizeTracesByColumn[colName].x.push(t);
            netsizeTracesByColumn[colName].y.push(val);
          }}
        }} else {{
          for (const colName of netsizeColumnsSet) {{
            netsizeTracesByColumn[colName].x.push(t);
            netsizeTracesByColumn[colName].y.push(0);
          }}
        }}
      }});
      const netsizeTraces = [];
      for (const [colName, arrObj] of Object.entries(netsizeTracesByColumn)) {{
        const dashIndex = colName.indexOf('-');
        let iface = colName;
        let direction = "";
        if (dashIndex > 0) {{
          iface = colName.substring(0, dashIndex);
          const afterDash = colName.substring(dashIndex+1);
          if (afterDash.startsWith("read")) {{
            direction = "readsize";
          }} else if (afterDash.startsWith("write")) {{
            direction = "writesize";
          }} else {{
            direction = afterDash;
          }}
        }}
        const traceName = iface + " " + direction;
        const clonedY = arrObj.y.map(v => v);
        if (direction === 'writesize') {{
          for (let i = 0; i < clonedY.length; i++) {{
            clonedY[i] = -Math.abs(clonedY[i]);
          }}
        }}
        netsizeTraces.push({{
          x: arrObj.x,
          y: clonedY,
          mode: 'lines',
          name: traceName
        }});
      }}
      Plotly.newPlot('netsize_chart', netsizeTraces, {{
        title: 'Network Size Read/Writesize (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time' }},
        yaxis: {{ title: 'Size (bytes)', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('netsize_chart'));
      // NEW: Network Errors & Collisions chart (NETERROR)
      const neterrorTracesByColumn = {{}};
      const neterrorColumnsSet = new Set();
      docs.forEach(d => {{
        if (d.neterror) {{
          for (const colName in d.neterror) {{
            neterrorColumnsSet.add(colName);
          }}
        }}
      }});
      neterrorColumnsSet.forEach(colName => {{
        neterrorTracesByColumn[colName] = {{ x: [], y: [] }};
      }});
      docs.forEach(d => {{
        const t = parseTimestamp(d["@timestamp"]);
        if (d.neterror) {{
          for (const colName of neterrorColumnsSet) {{
            const val = d.neterror[colName] !== undefined ? d.neterror[colName] : 0;
            neterrorTracesByColumn[colName].x.push(t);
            neterrorTracesByColumn[colName].y.push(val);
          }}
        }} else {{
          for (const colName of neterrorColumnsSet) {{
            neterrorTracesByColumn[colName].x.push(t);
            neterrorTracesByColumn[colName].y.push(0);
          }}
        }}
      }});
      const neterrorTraces = [];
      for (const [colName, arrObj] of Object.entries(neterrorTracesByColumn)) {{
        // Legends as "XX-ierrs", "XX-oerrs", "XX-collisions"
        neterrorTraces.push({{
          x: arrObj.x,
          y: arrObj.y,
          mode: 'lines',
          name: colName
        }});
      }}
      Plotly.newPlot('net_error_chart', neterrorTraces, {{
        title: 'Network Errors & Collisions (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time' }},
        yaxis: {{ title: 'Count', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('net_error_chart'));


      // 15) FC read/write
      const fcTracesByColumn = {{}};
      const fcColumnsSet = new Set();
      docs.forEach(d => {{
        if (d.fc) {{
          for (const colName in d.fc) {{
            fcColumnsSet.add(colName);
          }}
        }}
      }});
      fcColumnsSet.forEach(colName => {{
        fcTracesByColumn[colName] = {{ x: [], y: [] }};
      }});
      docs.forEach(d => {{
        const t = parseTimestamp(d["@timestamp"]);
        if (d.fc) {{
          for (const colName of fcColumnsSet) {{
            const val = d.fc[colName] !== undefined ? d.fc[colName] : 0;
            fcTracesByColumn[colName].x.push(t);
            fcTracesByColumn[colName].y.push(val);
          }}
        }} else {{
          for (const colName of fcColumnsSet) {{
            fcTracesByColumn[colName].x.push(t);
            fcTracesByColumn[colName].y.push(0);
          }}
        }}
      }});
      const fcTraces = [];
      for (const [colName, arrObj] of Object.entries(fcTracesByColumn)) {{
        const dashIndex = colName.indexOf('-');
        let iface = colName;
        let direction = "";
        if (dashIndex > 0) {{
          iface = colName.substring(0, dashIndex);
          direction = colName.substring(dashIndex+1);
        }}
        const traceName = iface + " " + direction;
        const clonedY = arrObj.y.map(v => v);
        if (direction === 'write') {{
          for (let i = 0; i < clonedY.length; i++) {{
            clonedY[i] = -Math.abs(clonedY[i]);
          }}
        }}
        fcTraces.push({{
          x: arrObj.x,
          y: clonedY,
          mode: 'lines',
          name: traceName
        }});
      }}
      Plotly.newPlot('fc_chart', fcTraces, {{
        title: 'Fibre Channel Read/Write (KB/s) (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time' }},
        yaxis: {{ title: 'KB/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('fc_chart'));
      // NEW: Fibre Channel Read/Write Summary chart (stacked mean/max pairs)
      const fcSummaryData = {{}};
      Object.entries(fcTracesByColumn).forEach(([colName, arrObj]) => {{
        const dash = colName.indexOf('-');
        let iface = colName;
        let direction = '';
        if (dash > 0) {{
          iface = colName.slice(0, dash);
          direction = colName.slice(dash + 1); // read / write
        }}
        if (!fcSummaryData[iface]) {{
          fcSummaryData[iface] = {{ read: [], write: [] }};
        }}
        if (direction === 'read')  fcSummaryData[iface].read.push(...arrObj.y);
        if (direction === 'write') fcSummaryData[iface].write.push(...arrObj.y.map(v => Math.abs(v)));
      }});
      const fcIfaces = Object.keys(fcSummaryData).sort();
      const meanRead = [], meanWrite = [], maxRead = [], maxWrite = [];
      fcIfaces.forEach(iface => {{
        const reads  = fcSummaryData[iface].read;
        const writes = fcSummaryData[iface].write;
        const mRead  = reads.length ? reads.reduce((a,b)=>a+b,0)/reads.length : 0;
        const mWrite = writes.length ? -(writes.reduce((a,b)=>a+b,0)/writes.length) : 0;
        const xRead  = reads.length ? Math.max(...reads) : 0;
        const xWrite = writes.length ? -Math.max(...writes) : 0;
        meanRead.push(mRead);
        meanWrite.push(mWrite);
        maxRead.push(xRead);
        maxWrite.push(xWrite);
      }});
      Plotly.newPlot('fc_summary_chart', [
        {{ x: fcIfaces, y: meanRead,  type:'bar', name:'Mean Read',
           marker:{{color:themeColor('--chart-1')}}, offsetgroup:'meanFC', legendgroup:'meanFC' }},
        {{ x: fcIfaces, y: meanWrite, type:'bar', name:'Mean Write',
           marker:{{color:themeColor('--success')}}, offsetgroup:'meanFC', legendgroup:'meanFC', base:0 }},
        {{ x: fcIfaces, y: maxRead,   type:'bar', name:'Max Read',
           marker:{{color:themeColor('--warning')}}, offsetgroup:'maxFC', legendgroup:'maxFC' }},
        {{ x: fcIfaces, y: maxWrite,  type:'bar', name:'Max Write',
           marker:{{color:themeColor('--danger')}}, offsetgroup:'maxFC', legendgroup:'maxFC', base:0 }}
      ], {{
        title: 'Fibre Channel Read/Write Summary (' + lparSelect.value + ')',
        barmode: 'group',
        bargap: 0.05,
        bargroupgap: 0.0,
        xaxis: {{ title:'Fibre Channel Interface' }},
        yaxis: {{ title:'KB/s', autorange:true }}
      }}).then(gd => linkCharts('fc_summary_chart'));


      // New: FC Stacked chart (separate stackgroups for read and write)
      const fcStackedTraces = [];
      let fcReadIndex = 0;
      let fcWriteIndex = 0;
      for (const [colName, arrObj] of Object.entries(fcTracesByColumn)) {{
        const dashIndex = colName.indexOf('-');
        let iface = colName;
        let direction = "";
        if (dashIndex > 0) {{
          iface = colName.substring(0, dashIndex);
          direction = colName.substring(dashIndex+1);
        }}
        const traceName = iface + " " + direction;
        const clonedY = arrObj.y.map(v => v);
        if (direction === 'write') {{
          for (let i = 0; i < clonedY.length; i++) {{
            clonedY[i] = -Math.abs(clonedY[i]);
          }}
        }}
        let fillMode;
        if (direction === 'read') {{
          fillMode = (fcReadIndex === 0) ? 'tozeroy' : 'tonexty';
          fcStackedTraces.push({{
            x: arrObj.x,
            y: clonedY,
            mode: 'lines',
            name: traceName,
            stackgroup: 'fc_stacked_read',
            fill: fillMode
          }});
          fcReadIndex++;
        }} else if (direction === 'write') {{
          fillMode = (fcWriteIndex === 0) ? 'tozeroy' : 'tonexty';
          fcStackedTraces.push({{
            x: arrObj.x,
            y: clonedY,
            mode: 'lines',
            name: traceName,
            stackgroup: 'fc_stacked_write',
            fill: fillMode
          }});
          fcWriteIndex++;
        }} else {{
          fillMode = 'tozeroy';
          fcStackedTraces.push({{
            x: arrObj.x,
            y: clonedY,
            mode: 'lines',
            name: traceName,
            stackgroup: 'fc_stacked',
            fill: fillMode
          }});
        }}
      }}
      Plotly.newPlot('fc_stacked_chart', fcStackedTraces, {{
        title: 'Fibre Channel Read/Write - Stacked (KB/s) (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time' }},
        yaxis: {{ title: 'KB/s' }}
      }}).then(gd => linkCharts('fc_stacked_chart'));

      // 16) FCXFERIN/FCXFEROUT
      const fcxferTracesByColumn = {{}};
      const fcxferColumnsSet = new Set();
      docs.forEach(d => {{
        if (d.fcxfer) {{
          for (const colName in d.fcxfer) {{
            fcxferColumnsSet.add(colName);
          }}
        }}
      }});
      fcxferColumnsSet.forEach(colName => {{
        fcxferTracesByColumn[colName] = {{ x: [], y: [] }};
      }});
      docs.forEach(d => {{
        const t = parseTimestamp(d["@timestamp"]);
        if (d.fcxfer) {{
          for (const colName of fcxferColumnsSet) {{
            const val = d.fcxfer[colName] !== undefined ? d.fcxfer[colName] : 0;
            fcxferTracesByColumn[colName].x.push(t);
            fcxferTracesByColumn[colName].y.push(val);
          }}
        }} else {{
          for (const colName of fcxferColumnsSet) {{
            fcxferTracesByColumn[colName].x.push(t);
            fcxferTracesByColumn[colName].y.push(0);
          }}
        }}
      }});
      const fcxferTraces = [];
      for (const [colName, arrObj] of Object.entries(fcxferTracesByColumn)) {{
        const dashIndex = colName.indexOf('-');
        let iface = colName;
        let direction = "";
        if (dashIndex > 0) {{
          iface = colName.substring(0, dashIndex);
          direction = colName.substring(dashIndex+1);
        }}
        const traceName = iface + " " + direction;
        const clonedY = arrObj.y.map(v => v);
        if (direction === 'out') {{
          for (let i = 0; i < clonedY.length; i++) {{
            clonedY[i] = -Math.abs(clonedY[i]);
          }}
        }}
        fcxferTraces.push({{
          x: arrObj.x,
          y: clonedY,
          mode: 'lines',
          name: traceName
        }});
      }}
      Plotly.newPlot('fcxfer_chart', fcxferTraces, {{
        title: 'Fibre Channel Xfers In/Out (fcs*) (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time' }},
        yaxis: {{ title: 'Transfers/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('fcxfer_chart'));
      
      // NEW: Fibre Channel Read/Write - Stacked by SAN ID (KB/s)
      (function() {{
        const currentSanByIface = {{}};
        const sanSeries = {{}};
        let hasSanSamples = false;

        docs.forEach(doc => {{
          if (doc.fc_san_map) {{
            Object.entries(doc.fc_san_map).forEach(([iface, sanId]) => {{
              if (sanId !== undefined && sanId !== null && String(sanId).trim() !== '') {{
                currentSanByIface[iface] = String(sanId).trim();
              }}
            }});
          }}
          const timestamp = parseTimestamp(doc["@timestamp"]);
          const sanTotals = {{}};
          fcColumnsSet.forEach(colName => {{
            const dash = colName.indexOf('-');
            let iface = colName;
            let direction = '';
            if (dash > 0) {{
              iface = colName.slice(0, dash);
              direction = colName.slice(dash + 1); // read / write
            }}
            const sanId = currentSanByIface[iface];
            if (!sanId) {{
              return;
            }}
            const val = (doc.fc && doc.fc[colName] !== undefined) ? doc.fc[colName] : 0;
            const key = sanId + '-' + direction;
            sanTotals[key] = (sanTotals[key] || 0) + (val || 0);
          }});

          if (Object.keys(sanTotals).length) {{
            if (!hasSanSamples) {{
              hasSanSamples = Object.values(sanTotals).some(v => Math.abs(v) > 1e-9);
            }}
          }}

          const keys = new Set(Object.keys(sanSeries));
          Object.keys(sanTotals).forEach(k => keys.add(k));
          keys.forEach(key => {{
            if (!sanSeries[key]) {{
              sanSeries[key] = {{ x: [], y: [] }};
            }}
            const hasValue = Object.prototype.hasOwnProperty.call(sanTotals, key);
            const value = hasValue ? sanTotals[key] : null;
            sanSeries[key].x.push(timestamp);
            sanSeries[key].y.push(value);
          }});
        }});

        if (!hasSanSamples) {{
          console.warn('No fc_san_map entries found; skipping Fibre Channel SAN charts');
          return;
        }}

        const sanLineTraces = [];
        const sanStackedTraces = [];
        const colorPalette = [
          themeColor('--chart-1'), themeColor('--chart-2'), themeColor('--chart-3'),
          themeColor('--chart-4'), themeColor('--chart-5'), themeColor('--chart-6'),
          themeColor('--chart-7'), themeColor('--chart-8'), themeColor('--chart-inspected'),
          themeColor('--chart-transferred')
        ];
        const hexToRgba = (hex, alpha) => {{
          let normalized = hex.replace('#', '');
          if (normalized.length === 3) {{
            normalized = normalized.split('').map(ch => ch + ch).join('');
          }}
          const intVal = parseInt(normalized, 16);
          const r = (intVal >> 16) & 255;
          const g = (intVal >> 8) & 255;
          const b = intVal & 255;
          return 'rgba(' + r + ',' + g + ',' + b + ',' + alpha + ')';
        }};
        let readIdx = 0;
        let writeIdx = 0;
        const sortedKeys = Object.keys(sanSeries).sort();
        sortedKeys.forEach((key, colorIndex) => {{
          const parts = key.split('-');
          const san = parts[0];
          const direction = parts[1];
          const arrObj = sanSeries[key];
          const lineY = arrObj.y.map(v => {{
            if (v === null || v === undefined) {{
              return null;
            }}
            const magnitude = Math.abs(v);
            const signed = direction === 'write' ? -magnitude : magnitude;
            return Math.abs(signed) < 1e-9 ? null : signed;
          }});
          const sanitizedY = lineY.map(v => (v === null ? null : (Math.abs(v) < 1e-9 ? null : v)));
          const color = colorPalette[colorIndex % colorPalette.length];
          const legendName = san + " " + direction;
          const legendGroup = "san_" + key;

          sanLineTraces.push({{
            x: arrObj.x,
            y: sanitizedY,
            mode: 'lines',
            name: legendName,
            connectgaps: false,
            line: {{ color }}
          }});
          const segments = [];
          let currentX = [];
          let currentY = [];
          sanitizedY.forEach((val, idx) => {{
            const xVal = arrObj.x[idx];
            if (val === null || val === undefined) {{
              if (currentX.length) {{
                segments.push({{ x: currentX, y: currentY }});
                currentX = [];
                currentY = [];
              }}
            }} else {{
              currentX.push(xVal);
              currentY.push(val);
            }}
          }});
          if (currentX.length) {{
            segments.push({{ x: currentX, y: currentY }});
          }}
          if (!segments.length) {{
            segments.push({{ x: [], y: [], empty: true }});
          }}
          const hasRealData = segments.some(seg => !seg.empty);
          const isWrite = direction === 'write';
          const stackGroupName = isWrite ? 'san_write' : 'san_read';
          const fillMode = isWrite
            ? (writeIdx === 0 ? 'tozeroy' : 'tonexty')
            : (readIdx === 0 ? 'tozeroy' : 'tonexty');
          if (hasRealData) {{
            if (isWrite) {{
              writeIdx++;
            }} else {{
              readIdx++;
            }}
          }}
          segments.forEach((segment, segIdx) => {{
            sanStackedTraces.push({{
              x: segment.x,
              y: segment.y,
              mode: 'lines',
              name: legendName,
              stackgroup: stackGroupName,
              fill: fillMode,
              connectgaps: false,
              showlegend: segIdx === 0,
              legendgroup: legendGroup,
              line: {{ color }},
              fillcolor: hexToRgba(color, 0.6),
              visible: segment.empty ? 'legendonly' : undefined
            }});
          }});
        }});

        Plotly.newPlot('fc_san_chart', sanLineTraces, {{
          title: 'Fibre Channel Read/Write by SAN ID (KB/s) (' + lparSelect.value + ')',
          xaxis: {{ title: 'Time' }},
          yaxis: {{ title: 'KB/s' }}
        }}).then(gd => linkCharts('fc_san_chart'));
        Plotly.newPlot('fc_stacked_san_chart', sanStackedTraces, {{
          title: 'Fibre Channel Read/Write - Stacked by SAN ID (KB/s) (' + lparSelect.value + ')',
          xaxis: {{ title: 'Time' }},
          yaxis: {{ title: 'KB/s' }}
        }}).then(gd => linkCharts('fc_stacked_san_chart'));
      }})();
    

      // 17) DISK read/write
      const diskNamesSet = new Set();
      docs.forEach(d => {{
        if (d.diskread) {{
          for (const diskName in d.diskread) {{
            diskNamesSet.add(diskName);
          }}
        }}
        if (d.diskwrite) {{
          for (const diskName in d.diskwrite) {{
            diskNamesSet.add(diskName);
          }}
        }}
      }});
      const diskRWTraces = [];
      for (const diskName of diskNamesSet) {{
        const xVals = [];
        const yRead = [];
        const yWrite = [];
        docs.forEach(doc => {{
          xVals.push(parseTimestamp(doc["@timestamp"]));
          let rd = 0;
          let wt = 0;
          if (doc.diskread && doc.diskread[diskName] !== undefined) {{
            rd = doc.diskread[diskName];
          }}
          if (doc.diskwrite && doc.diskwrite[diskName] !== undefined) {{
            wt = doc.diskwrite[diskName];
          }}
          yRead.push(rd);
          yWrite.push(-Math.abs(wt));
        }});
        diskRWTraces.push({{
          x: xVals,
          y: yRead,
          mode: 'lines',
          name: diskName + " read"
        }});
        diskRWTraces.push({{
          x: xVals,
          y: yWrite,
          mode: 'lines',
          name: diskName + " write"
        }});
      }}
      Plotly.newPlot('disk_read_write_chart', diskRWTraces, {{
        title: 'DISK Read/Write (KB/s)',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'KB/s' }}
      }}).then(gd => linkCharts('disk_read_write_chart'));

      // New: DISK Read/Write Stacked chart (separate stackgroups for read and write)
      const diskStackedTraces = [];
      let diskReadIndex = 0;
      let diskWriteIndex = 0;
      for (const diskName of diskNamesSet) {{
        const xVals = [];
        const yRead = [];
        const yWrite = [];
        docs.forEach(doc => {{
          xVals.push(parseTimestamp(doc["@timestamp"]));
          let rd = 0;
          let wt = 0;
          if (doc.diskread && doc.diskread[diskName] !== undefined) {{
            rd = doc.diskread[diskName];
          }}
          if (doc.diskwrite && doc.diskwrite[diskName] !== undefined) {{
            wt = doc.diskwrite[diskName];
          }}
          yRead.push(rd);
          yWrite.push(-Math.abs(wt));
        }});
        let fillModeRead = (diskReadIndex === 0) ? 'tozeroy' : 'tonexty';
        diskStackedTraces.push({{
          x: xVals,
          y: yRead,
          mode: 'lines',
          name: diskName + " read",
          stackgroup: 'disk_stacked_read',
          fill: fillModeRead
        }});
        diskReadIndex++;
        let fillModeWrite = (diskWriteIndex === 0) ? 'tozeroy' : 'tonexty';
        diskStackedTraces.push({{
          x: xVals,
          y: yWrite,
          mode: 'lines',
          name: diskName + " write",
          stackgroup: 'disk_stacked_write',
          fill: fillModeWrite
        }});
        diskWriteIndex++;
      }}
      Plotly.newPlot('disk_read_write_stacked_chart', diskStackedTraces, {{
        title: 'DISK Read/Write - Stacked (KB/s)',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'KB/s' }}
      }}).then(gd => linkCharts('disk_read_write_stacked_chart'));

      // 18) DISK busy
      const diskBusyNames = new Set();
      docs.forEach(d => {{
        if (d.diskbusy) {{
          for (const diskName in d.diskbusy) {{
            diskBusyNames.add(diskName);
          }}
        }}
      }});
      const diskBusyTraces = [];
      for (const diskName of diskBusyNames) {{
        const xVals = [];
        const yVals = [];
        docs.forEach(doc => {{
          xVals.push(parseTimestamp(doc["@timestamp"]));
          let val = 0;
          if (doc.diskbusy && doc.diskbusy[diskName] !== undefined) {{
            val = doc.diskbusy[diskName];
          }}
          yVals.push(val);
        }});
        diskBusyTraces.push({{
          x: xVals,
          y: yVals,
          mode: 'lines',
          name: diskName
        }});
      }}
      Plotly.newPlot('disk_busy_chart', diskBusyTraces, {{
        title: 'DISK Busy (%)',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: '%Busy', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('disk_busy_chart'));

      // 19) DISK wait
      const diskWaitNames = new Set();
      docs.forEach(d => {{
        if (d.diskwait) {{
          for (const diskName in d.diskwait) {{
            diskWaitNames.add(diskName);
          }}
        }}
      }});
      const diskWaitTraces = [];
      for (const diskName of diskWaitNames) {{
        const xVals = [];
        const yVals = [];
        docs.forEach(doc => {{
          xVals.push(parseTimestamp(doc["@timestamp"]));
          let val = 0;
          if (doc.diskwait && doc.diskwait[diskName] !== undefined) {{
            val = doc.diskwait[diskName];
          }}
          yVals.push(val);
        }});
        diskWaitTraces.push({{
          x: xVals,
          y: yVals,
          mode: 'lines',
          name: diskName
        }});
      }}
      Plotly.newPlot('disk_wait_chart', diskWaitTraces, {{
        title: 'DISK Wait (msec/xfer)',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Wait Time (msec/xfer)', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('disk_wait_chart'));

      // 20) VG read/write
      const vgNamesSet = new Set();
      docs.forEach(d => {{
        if (d.vgread) {{
          for (const vgName in d.vgread) {{
            vgNamesSet.add(vgName);
          }}
        }}
        if (d.vgwrite) {{
          for (const vgName in d.vgwrite) {{
            vgNamesSet.add(vgName);
          }}
        }}
      }});
      const vgRWTraces = [];
      for (const vgName of vgNamesSet) {{
        const xVals = [];
        const yRead = [];
        const yWrite = [];
        docs.forEach(doc => {{
          xVals.push(parseTimestamp(doc["@timestamp"]));
          let rd = 0;
          let wt = 0;
          if (doc.vgread && doc.vgread[vgName] !== undefined) {{
            rd = doc.vgread[vgName];
          }}
          if (doc.vgwrite && doc.vgwrite[vgName] !== undefined) {{
            wt = doc.vgwrite[vgName];
          }}
          yRead.push(rd);
          yWrite.push(-Math.abs(wt));
        }});
        vgRWTraces.push({{
          x: xVals,
          y: yRead,
          mode: 'lines',
          name: vgName + " read"
        }});
        vgRWTraces.push({{
          x: xVals,
          y: yWrite,
          mode: 'lines',
          name: vgName + " write"
        }});
      }}
      Plotly.newPlot('vg_read_write_chart', vgRWTraces, {{
        title: 'VG Read/Write (KB/s)',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'KB/s' }}
      }}).then(gd => linkCharts('vg_read_write_chart'));

      // New: VG Read/Write Stacked chart (separate stackgroups for read and write)
      const vgStackedTraces = [];
      let vgReadIndex = 0;
      let vgWriteIndex = 0;
      for (const vgName of vgNamesSet) {{
        const xVals = [];
        const yRead = [];
        const yWrite = [];
        docs.forEach(doc => {{
          xVals.push(parseTimestamp(doc["@timestamp"]));
          let rd = 0;
          let wt = 0;
          if (doc.vgread && doc.vgread[vgName] !== undefined) {{
            rd = doc.vgread[vgName];
          }}
          if (doc.vgwrite && doc.vgwrite[vgName] !== undefined) {{
            wt = doc.vgwrite[vgName];
          }}
          yRead.push(rd);
          yWrite.push(-Math.abs(wt));
        }});
        let fillModeRead = (vgReadIndex === 0) ? 'tozeroy' : 'tonexty';
        vgStackedTraces.push({{
          x: xVals,
          y: yRead,
          mode: 'lines',
          name: vgName + " read",
          stackgroup: 'vg_stacked_read',
          fill: fillModeRead
        }});
        vgReadIndex++;
        let fillModeWrite = (vgWriteIndex === 0) ? 'tozeroy' : 'tonexty';
        vgStackedTraces.push({{
          x: xVals,
          y: yWrite,
          mode: 'lines',
          name: vgName + " write",
          stackgroup: 'vg_stacked_write',
          fill: fillModeWrite
        }});
        vgWriteIndex++;
      }}
      Plotly.newPlot('vg_read_write_stacked_chart', vgStackedTraces, {{
        title: 'VG Read/Write - Stacked (KB/s)',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'KB/s' }}
      }}).then(gd => linkCharts('vg_read_write_stacked_chart'));

      // 21) VG busy
      const vgBusyNames = new Set();
      docs.forEach(d => {{
        if (d.vgbusy) {{
          for (const vgName in d.vgbusy) {{
            vgBusyNames.add(vgName);
          }}
        }}
      }});
      const vgBusyTraces = [];
      for (const vgName of vgBusyNames) {{
        const xVals = [];
        const yVals = [];
        docs.forEach(doc => {{
          xVals.push(parseTimestamp(doc["@timestamp"]));
          let val = 0;
          if (doc.vgbusy && doc.vgbusy[vgName] !== undefined) {{
            val = doc.vgbusy[vgName];
          }}
          yVals.push(val);
        }});
        vgBusyTraces.push({{
          x: xVals,
          y: yVals,
          mode: 'lines',
          name: vgName
        }});
      }}
      Plotly.newPlot('vg_busy_chart', vgBusyTraces, {{
        title: 'VG Busy (%)',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: '%Busy' }}
      }}).then(gd => linkCharts('vg_busy_chart'));

      // 22) JFS Percent Full
      const jfsfileTracesByColumn = {{}};
      docs.forEach(d => {{
        if (d.jfsfile) {{
          for (const fs in d.jfsfile) {{
            if (!jfsfileTracesByColumn[fs]) {{
              jfsfileTracesByColumn[fs] = {{ x: [], y: [] }};
            }}
            jfsfileTracesByColumn[fs].x.push(parseTimestamp(d["@timestamp"]));
            jfsfileTracesByColumn[fs].y.push(d.jfsfile[fs]);
          }}
        }}
      }});
      const jfsTraces = [];
      for (const [fs, arrObj] of Object.entries(jfsfileTracesByColumn)) {{
        jfsTraces.push({{
          x: arrObj.x,
          y: arrObj.y,
          mode: 'lines',
          name: fs
        }});
      }}
      Plotly.newPlot('jfs_percent_full_chart', jfsTraces, {{
        title: 'JFS Percent Full (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'Percentage', range: [0, 100] }}
      }}).then(gd => linkCharts('jfs_percent_full_chart'));

// NEW: LPM Migration Chart
const lpmDocs = getFilteredLpmDocs();
if (!lpmDocs.length) {{
  document.getElementById('lpm_chart').innerHTML = '<p>No LPM data</p>';
}} else {{
  // Create an array of y-axis categories in chronological order
  const yCategoryLabels = [...new Set(lpmDocs.map(d => `${{d.old_serial}} -> ${{d.current_serial}}`))];

  const traces = {{
    check: {{ x: [], y: [], mode: 'markers', type: 'scatter', name: 'check', marker: {{ color: themeColor('--chart-insp-count') }} }},
    pre: {{ x: [], y: [], mode: 'markers', type: 'scatter', name: 'pre', marker: {{ color: themeColor('--warning'), line: {{ color: themeColor('--text'), width: 1 }} }} }},
    post: {{ x: [], y: [], mode: 'markers', type: 'scatter', name: 'post', marker: {{ color: themeColor('--success') }} }},
    posterror: {{ x: [], y: [], mode: 'markers', type: 'scatter', name: 'posterror', marker: {{ color: themeColor('--danger') }} }}
  }};

  lpmDocs.forEach(d => {{
    const state = d.state;
    if (traces[state]) {{
      traces[state].x.push(parseTimestamp(d['@timestamp']));
      const y_val = `${{d.old_serial}} -> ${{d.current_serial}}`;
      traces[state].y.push(y_val);
    }}
  }});

  Plotly.newPlot('lpm_chart', Object.values(traces), {{
    title: 'LPM Migration Events (' + lparSelect.value + ')',
    xaxis: {{ title: 'Time', range: xRange }},
    yaxis: {{
      title: 'Serial Number Transition',
      type: 'category',
      automargin: true,
      // --- Add these two lines to enforce chronological order ---
      categoryorder: 'array',
      categoryarray: yCategoryLabels
    }}
  }}).then(gd => linkCharts('lpm_chart'));
}}


      // NEW: SEA (READ/WRITE (KB/s)) chart (unstacked)
      const seaTracesByInterface = {{}};
      docs.forEach(d => {{
        if (d.sea) {{
          const time = parseTimestamp(d["@timestamp"]);
          for (const colName in d.sea) {{
            // Expecting keys like "ent25-read-KB/s" or "ent25-write-KB/s"
            const parts = colName.split('-');
            const iface = parts[0];
            const metric = parts[1];
            if (!seaTracesByInterface[iface]) {{
              seaTracesByInterface[iface] = {{ read: {{ x: [], y: [] }}, write: {{ x: [], y: [] }} }};
            }}
            if (metric.startsWith("read")) {{
              seaTracesByInterface[iface].read.x.push(time);
              seaTracesByInterface[iface].read.y.push(d.sea[colName]);
            }} else if (metric.startsWith("write")) {{
              seaTracesByInterface[iface].write.x.push(time);
              seaTracesByInterface[iface].write.y.push(-Math.abs(d.sea[colName]));
            }}
          }}
        }}
      }});
      const seaTraces = [];
      for (const iface in seaTracesByInterface) {{
        seaTraces.push({{
          x: seaTracesByInterface[iface].read.x,
          y: seaTracesByInterface[iface].read.y,
          mode: 'lines',
          name: iface + " read"
        }});
        seaTraces.push({{
          x: seaTracesByInterface[iface].write.x,
          y: seaTracesByInterface[iface].write.y,
          mode: 'lines',
          name: iface + " write",
        }});
      }}
      Plotly.newPlot('sea_chart', seaTraces, {{
        title: 'SEA (READ/WRITE (KB/s)) (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'KB/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('sea_chart'));
      // NEW: SEA Read/Write Summary chart (stacked mean/max pairs)
      const seaSummaryData = {{}};
      Object.entries(seaTracesByInterface).forEach(([iface, obj]) => {{
          seaSummaryData[iface] = {{
              read:  obj.read.y.slice(),
              write: obj.write.y.map(v => Math.abs(v))
          }};
      }});
      const seaIfaces = Object.keys(seaSummaryData).sort();
      const seaMeanRead = [], seaMeanWrite = [], seaMaxRead = [], seaMaxWrite = [];
      seaIfaces.forEach(iface => {{
          const reads = seaSummaryData[iface].read;
          const writes = seaSummaryData[iface].write;
          const mRead = reads.length ? reads.reduce((a,b)=>a+b,0)/reads.length : 0;
          const mWrite = writes.length ? -(writes.reduce((a,b)=>a+b,0)/writes.length) : 0;
          const xRead = reads.length ? Math.max(...reads) : 0;
          const xWrite = writes.length ? -Math.max(...writes) : 0;
          seaMeanRead.push(mRead);
          seaMeanWrite.push(mWrite);
          seaMaxRead.push(xRead);
          seaMaxWrite.push(xWrite);
      }});
      Plotly.newPlot('sea_summary_chart', [
          {{ x: seaIfaces, y: seaMeanRead,  type:'bar', name:'Mean Read',
             marker:{{color:themeColor('--chart-1')}}, offsetgroup:'meanSEA', legendgroup:'meanSEA' }},
          {{ x: seaIfaces, y: seaMeanWrite, type:'bar', name:'Mean Write',
             marker:{{color:themeColor('--success')}}, offsetgroup:'meanSEA', legendgroup:'meanSEA', base:0 }},
          {{ x: seaIfaces, y: seaMaxRead,   type:'bar', name:'Max Read',
             marker:{{color:themeColor('--warning')}}, offsetgroup:'maxSEA', legendgroup:'maxSEA' }},
          {{ x: seaIfaces, y: seaMaxWrite,  type:'bar', name:'Max Write',
             marker:{{color:themeColor('--danger')}}, offsetgroup:'maxSEA', legendgroup:'maxSEA', base:0 }}
      ], {{
          title: 'SEA Read/Write Summary (' + lparSelect.value + ')',
          barmode: 'group',
          bargap: 0.05,
          bargroupgap: 0.0,
          xaxis: {{ title:'SEA Interface' }},
          yaxis: {{ title:'KB/s', autorange:true }}
      }}).then(gd => linkCharts('sea_summary_chart'));


      // NEW: SEA Read/Write - Stacked (KB/s) chart
      const seaStackedTraces = [];
      let firstRead = true;
      let firstWrite = true;
      for (const iface in seaTracesByInterface) {{
          let readFill = firstRead ? 'tozeroy' : 'tonexty';
          firstRead = false;
          let writeFill = firstWrite ? 'tozeroy' : 'tonexty';
          firstWrite = false;
          seaStackedTraces.push({{
            x: seaTracesByInterface[iface].read.x,
            y: seaTracesByInterface[iface].read.y,
            mode: 'lines',
            name: iface + " read",
            stackgroup: 'sea_stacked_read',
            fill: readFill
          }});
          seaStackedTraces.push({{
            x: seaTracesByInterface[iface].write.x,
            y: seaTracesByInterface[iface].write.y,
            mode: 'lines',
            name: iface + " write",
            stackgroup: 'sea_stacked_write',
            fill: writeFill
          }});
      }}
      Plotly.newPlot('sea_stacked_chart', seaStackedTraces, {{
         title: 'SEA Read/Write - Stacked (KB/s) (' + lparSelect.value + ')',
         xaxis: {{ title: 'Time', range: xRange }},
         yaxis: {{ title: 'KB/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('sea_stacked_chart'));

      // NEW: SEA Packets/s chart
      const seapacketTracesByInterface = {{}};
      docs.forEach(d => {{
        if (d.seapacket) {{
          const time = parseTimestamp(d["@timestamp"]);
          for (const colName in d.seapacket) {{
            // Expecting keys like "ent25-reads/s" or "ent25-writes/s"
            const parts = colName.split('-');
            const iface = parts[0];
            const metric = parts[1];
            if (!seapacketTracesByInterface[iface]) {{
              seapacketTracesByInterface[iface] = {{ read: {{ x: [], y: [] }}, write: {{ x: [], y: [] }} }};
            }}
            if(metric.startsWith("read")) {{
              seapacketTracesByInterface[iface].read.x.push(time);
              seapacketTracesByInterface[iface].read.y.push(d.seapacket[colName]);
            }} else if(metric.startsWith("write")) {{
              seapacketTracesByInterface[iface].write.x.push(time);
              seapacketTracesByInterface[iface].write.y.push(-Math.abs(d.seapacket[colName]));
            }}
          }}
        }}
      }});
      const seapacketTraces = [];
      for (const iface in seapacketTracesByInterface) {{
        seapacketTraces.push({{
          x: seapacketTracesByInterface[iface].read.x,
          y: seapacketTracesByInterface[iface].read.y,
          mode: 'lines',
          name: iface + " read"
        }});
        seapacketTraces.push({{
          x: seapacketTracesByInterface[iface].write.x,
          y: seapacketTracesByInterface[iface].write.y,
          mode: 'lines',
          name: iface + " write"
        }});
      }}
      Plotly.newPlot('sea_packet_chart', seapacketTraces, {{
         title: 'SEA Packets/s (' + lparSelect.value + ')',
         xaxis: {{ title: 'Time', range: xRange }},
         yaxis: {{ title: 'Packets/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('sea_packet_chart'));
      // NEW: SEAPHY (READ/WRITE (KB/s)) chart
      const seaphyTracesByInterface = {{}};
      docs.forEach(d => {{
        if (d.seachphy) {{
          const time = parseTimestamp(d["@timestamp"]);
          for (const colName in d.seachphy) {{
            if (colName.endsWith('_read-KB/s') || colName.endsWith('_write-KB/s')) {{
              const parts = colName.split('_');
              const iface = parts[0];
              const metric = parts[1]; // read-KB/s or write-KB/s
              if (!seaphyTracesByInterface[iface]) {{
                seaphyTracesByInterface[iface] = {{ read: {{ x: [], y: [] }}, write: {{ x: [], y: [] }} }};
              }}
              if (metric.startsWith('read')) {{
                seaphyTracesByInterface[iface].read.x.push(time);
                seaphyTracesByInterface[iface].read.y.push(d.seachphy[colName]);
              }} else if (metric.startsWith('write')) {{
                seaphyTracesByInterface[iface].write.x.push(time);
                seaphyTracesByInterface[iface].write.y.push(-Math.abs(d.seachphy[colName]));
              }}
            }}
          }}
        }}
      }});
      const seaphyTraces = [];
      for (const iface in seaphyTracesByInterface) {{
        seaphyTraces.push({{
          x: seaphyTracesByInterface[iface].read.x,
          y: seaphyTracesByInterface[iface].read.y,
          mode: 'lines',
          name: iface + ' read'
        }});
        seaphyTraces.push({{
          x: seaphyTracesByInterface[iface].write.x,
          y: seaphyTracesByInterface[iface].write.y,
          mode: 'lines',
          name: iface + ' write'
        }});
      }}
      Plotly.newPlot('sea_phy_rw_chart', seaphyTraces, {{
        title: 'SEAPHY (READ/WRITE KB/s) (' + lparSelect.value + ')',
        xaxis: {{ title: 'Time', range: xRange }},
        yaxis: {{ title: 'KB/s', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('sea_phy_rw_chart'));

      // NEW: SEAPHY Read/Write Summary chart
      const seaphySummaryData = {{}};
      Object.entries(seaphyTracesByInterface).forEach(([iface,obj])=>{{
        seaphySummaryData[iface] = {{
          read: obj.read.y.slice(),
          write: obj.write.y.map(v=>Math.abs(v))
        }};
      }});
      const seaphyIfaces = Object.keys(seaphySummaryData).sort();
      const seaphyMeanRead=[], seaphyMeanWrite=[], seaphyMaxRead=[], seaphyMaxWrite=[];
      seaphyIfaces.forEach(iface=>{{
        const reads = seaphySummaryData[iface].read;
        const writes = seaphySummaryData[iface].write;
        const mRead = reads.length ? reads.reduce((a,b)=>a+b,0)/reads.length : 0;
        const mWrite = writes.length ? -(writes.reduce((a,b)=>a+b,0)/writes.length) : 0;
        const xRead = reads.length ? Math.max(...reads) : 0;
        const xWrite = writes.length ? -Math.max(...writes) : 0;
        seaphyMeanRead.push(mRead);
        seaphyMeanWrite.push(mWrite);
        seaphyMaxRead.push(xRead);
        seaphyMaxWrite.push(xWrite);
      }});
      Plotly.newPlot('sea_phy_summary_chart', [
        {{ x: seaphyIfaces, y: seaphyMeanRead, type:'bar', name:'Mean Read',
          marker:{{color:themeColor('--chart-1')}}, offsetgroup:'meanSP', legendgroup:'meanSP' }},
        {{ x: seaphyIfaces, y: seaphyMeanWrite, type:'bar', name:'Mean Write',
          marker:{{color:themeColor('--success')}}, offsetgroup:'meanSP', legendgroup:'meanSP', base:0 }},
        {{ x: seaphyIfaces, y: seaphyMaxRead, type:'bar', name:'Max Read',
          marker:{{color:themeColor('--warning')}}, offsetgroup:'maxSP', legendgroup:'maxSP' }},
        {{ x: seaphyIfaces, y: seaphyMaxWrite, type:'bar', name:'Max Write',
          marker:{{color:themeColor('--danger')}}, offsetgroup:'maxSP', legendgroup:'maxSP', base:0 }}
      ], {{
        title: 'SEAPHY Read/Write Summary (' + lparSelect.value + ')',
        barmode: 'group',
        bargap: 0.05,
        bargroupgap: 0.0,
        xaxis: {{ title:'SEA PHY Interface' }},
        yaxis: {{ title:'KB/s', autorange:true }}
      }}).then(gd => linkCharts('sea_phy_summary_chart'));

      // NEW: SEA PHY Errors (Transmit/Receive) chart
      const seaPhyTransmitErr = [];
      const seaPhyReceiveErr = [];
      docs.forEach(d => {{
          let txErr = 0; let rxErr = 0;
          if (d.seachphy) {{
              for (const colName in d.seachphy) {{
                  if (colName.endsWith('_Transmit_Errors')) txErr += d.seachphy[colName];
                  if (colName.endsWith('_Receive_Errors')) rxErr += d.seachphy[colName];
              }}
          }}
          seaPhyTransmitErr.push(txErr);
          seaPhyReceiveErr.push(rxErr);
      }});
      Plotly.newPlot('sea_phy_error_chart', [
          {{ x: times, y: seaPhyTransmitErr, mode: 'lines', name: 'Transmit Errors' }},
          {{ x: times, y: seaPhyReceiveErr, mode: 'lines', name: 'Receive Errors' }}
      ], {{
          title: 'SEA PHY Errors (Transmit/Receive) (' + lparSelect.value + ')',
          xaxis: {{ title: 'Time', range: xRange }},
          yaxis: {{ title: 'Errors', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('sea_phy_error_chart'));

      // NEW: SEA PHY Packets Dropped chart
      const seaPhyDrops = [];
      docs.forEach(d => {{
          let drops = 0;
          if (d.seachphy) {{
              for (const colName in d.seachphy) {{
                  if (colName.endsWith('_Packets_Dropped')) drops += d.seachphy[colName];
              }}
          }}
          seaPhyDrops.push(drops);
      }});
      Plotly.newPlot('sea_phy_drop_chart', [
          {{ x: times, y: seaPhyDrops, mode: 'lines', name: 'Packets Dropped' }}
      ], {{
          title: 'SEA PHY Packets Dropped (' + lparSelect.value + ')',
          xaxis: {{ title: 'Time', range: xRange }},
          yaxis: {{ title: 'Packets', rangemode: 'tozero' }}
      }}).then(gd => linkCharts('sea_phy_drop_chart'));

      
       applyDarkModeToAllCharts();
    }}

    function applyFilter() {{
      renderCharts();
    }}

    document.getElementById("chartsPerRow").addEventListener("change", renderCharts);
    lparSelect.addEventListener("change", renderCharts);
    frameSelect.addEventListener("change", () => {{ populateLpars(); renderCharts(); }});

    // initial rendering, then link all charts
    renderCharts();
    setupFullscreen();
  </script>

<style>
.comparison {{ pointer-events:auto !important; }}
</style>
<script>
document.addEventListener('DOMContentLoaded', () => {{
  /* ------ Globals ------ */
  let comparisonMode = false;
  const chartIds = [...document.querySelectorAll('#chartsContainer > div > div')].map(d=>d.id);

  /* ------ Build secondary menu ------ */
  const menuA = document.querySelector('.menu');
  const menuB = menuA.cloneNode(true);
  menuB.id = 'menu_b';
  menuB.querySelectorAll('[id]').forEach(el => el.id += '_b');
  menuB.querySelectorAll('[for]').forEach(el => {{ if (el.htmlFor) el.htmlFor += '_b'; }});
  const compareLabel = menuB.querySelector('.chart-filter-label');
  if (compareLabel) compareLabel.textContent = 'Compare filters';
  menuB.style.display = 'none';
  menuB.style.position = 'static';
  menuB.style.top = '';
  menuA.after(menuB);

  /* fill FRAME & LPAR list B */
  const selA = document.getElementById('lpar_select');
  const selB = document.getElementById('lpar_select_b');
  const frameA = document.getElementById('frame_select');
  const frameB = document.getElementById('frame_select_b');

  frameB.innerHTML = '';
  Array.from(new Set(Object.values(frameMap))).sort().forEach(fr => {{ const o=document.createElement('option'); o.value=fr; o.textContent=fr; frameB.appendChild(o); }});
  frameB.value = frameA.value;

  function populateLparsB() {{
      selB.innerHTML = '';
      Object.keys(frameMap).sort().forEach(n => {{ if(frameMap[n] === frameB.value){{ const o=document.createElement('option'); o.value=n; o.textContent=n; selB.appendChild(o); }} }});
      if(!selB.value && selB.options.length) selB.value = selB.options[0].value;
  }}

  populateLparsB();
  selB.value = selA.value;

  /* ---- create empty comparison divs ---- */
  chartIds.forEach(id=>{{
      const box=document.getElementById(id).parentElement;
      const clone=box.cloneNode(false); // shallow
      const inner=document.createElement('div'); inner.id=id+'_b';
      clone.appendChild(inner);
      clone.classList.add('comparison');
      clone.style.display='none';
      box.after(clone);
  }});

  function filterDocs(lpar,start,end,map){{
      let d=(map[lpar]||[]).slice();
      if(start) d=d.filter(x=>parseTimestamp(x['@timestamp'])>=new Date(start));
      if(end){{ const e=new Date(end);e.setHours(23,59,59,999); d=d.filter(x=>parseTimestamp(x['@timestamp'])<=e);}}
      return d.sort((a,b)=>parseTimestamp(a['@timestamp'])-parseTimestamp(b['@timestamp']));
  }}

  /* clone full figure from A to B */
  function copyFigure(idSuffix){{
      const fig=Plotly.Plots.graphJson(document.getElementById(idSuffix.replace('_b','')));
      Plotly.newPlot(idSuffix, fig.data, fig.layout, {{displayModeBar:true, responsive:true}});
  }}

  function renderChartsB(){{
      const lpar=selB.value;
      const start=document.getElementById('start_date_b').value;
      const end=document.getElementById('end_date_b').value;

      /* temporarily override data helpers used by original renderCharts */
      const origGet=getFilteredDocs, origTop=getFilteredTopDocs;
      getFilteredDocs=()=>filterDocs(lpar,start,end,lparDataMap);
      getFilteredTopDocs=()=>filterDocs(lpar,start,end,topDataMap);

      /* call original renderer but target hidden div ids */
      chartIds.forEach(id=>{{document.getElementById(id).id=id+'_temp';}});
      chartIds.forEach(id=>{{document.getElementById(id+'_b').id=id;}});
      renderCharts();                                         // draw into B
      chartIds.forEach(id=>{{document.getElementById(id).id=id+'_b';}});
      chartIds.forEach(id=>{{document.getElementById(id+'_temp').id=id;}});

      /* restore helpers */
      getFilteredDocs=origGet; getFilteredTopDocs=origTop;

      /* ensure modebars present */
      chartIds.forEach(id=>copyFigure(id+'_b'));
  }}

  /* toggle comparison */
  document.getElementById('compareToggle').addEventListener('click', ()=>{{
      comparisonMode=!comparisonMode;
      document.body.classList.toggle('comparison-mode',comparisonMode);
      const compareBtn = document.getElementById('compareToggle');
      compareBtn.classList.toggle('active', comparisonMode);
      compareBtn.setAttribute('aria-pressed', comparisonMode ? 'true' : 'false');
      menuB.style.display=comparisonMode?'block':'none';
      document.querySelectorAll('.comparison').forEach(e=>e.style.display=comparisonMode?'block':'none');
      if(comparisonMode){{ renderChartsB(); }}
  }});

  /* change listeners */
  ['frame_select_b','lpar_select_b','start_date_b','end_date_b'].forEach(id=>{{
     document.getElementById(id).addEventListener('change',()=>{{ if(id==='frame_select_b'){{ populateLparsB(); }} if(comparisonMode){{ renderChartsB(); }} }});
  }});

  /* dark mode observer */
  const obs=new MutationObserver(()=>{{
    const layout = plotlyThemeRelayout();
    [...chartIds, ...chartIds.map(i=>i+'_b')].forEach(cid=>{{ const el=document.getElementById(cid); if(el && el.data) Plotly.relayout(el,layout);}});
  }});
  obs.observe(document.body,{{attributes:true,attributeFilter:['class']}});
}});
</script>
</body>
</html>"""

    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html_content)
    print("Wrote HTML (16 existing charts + DISK/VG charts, plus new Paging, FS Cache, unstacked SEA, stacked SEA, SEA Packets/s, MEM MB, Top PID, CPU Use, Bubble, InterProcess Comms, and LPM charts) to:", output_html)

################################################################################
# 5. process_file => parse => NDJSON => return
################################################################################

def process_file(nmon_file, output_dir):
    # Determine frame (SerialNumber) for this nmon file
    frame = None
    with open(nmon_file, 'r', encoding='utf-8' , errors='ignore') as meta_f:
        for line in meta_f:
            if line.startswith('AAA,SerialNumber'):
                parts = line.strip().split(',')
                if len(parts) > 2:
                    frame = parts[2].strip()
                break

    (
        cpu_data,
        lpar_data,
        proc_data,
        file_io_data,
        top_data_by_tag,
        zzzz_map,
        node,
        memnew_data_by_tag,
        mem_data_by_tag,
        mem_mb_data_by_tag,  # NEW: MEM MB data
        net_data_by_tag,
        netpacket_data_by_tag,
        diskread_data_by_tag,
        diskwrite_data_by_tag,
        diskbusy_data_by_tag,
        diskwait_data_by_tag,
        vgread_data_by_tag,
        vgwrite_data_by_tag,
        vgbusy_data_by_tag,
        vgsize_data_by_tag,
        jfsfile_data_by_tag,
        memuse_data_by_tag,   # NEW: FS Cache Memory Use data
        page_data_by_tag,     # NEW: Paging data
        sea_data_by_tag,      # NEW: SEA data
        seachphy_data_by_tag,      # NEW: SEA PHY Errors & Drops data
        seapacket_data_by_tag, # NEW: SEA Packets/s data
        cpu_use_data_by_tag,   # NEW: CPU Use per logical CPU data
        lpm_data
    ) = parse_nmon_file(nmon_file)

    # --- Already-existing logic for "fc" read/write and "netsize" --- 
    # We do NOT remove or change it. We only add the new "FCXFERIN"/"FCXFEROUT" pass.

    fc_by_tag = {}
    fc_read_header = []
    fc_write_header = []
    with open(nmon_file, 'r', encoding='utf-8' , errors='ignore') as f2:
        for line in f2:
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            key = parts[0]
            if key == 'FCREAD' and len(parts) > 2 and not parts[1].startswith('T'):
                fc_read_header = parts[2:]
                continue
            if key == 'FCWRITE' and len(parts) > 2 and not parts[1].startswith('T'):
                fc_write_header = parts[2:]
                continue
            if key == 'FCREAD' and len(parts) > 2 and parts[1].startswith('T'):
                tag = parts[1]
                numeric_vals = []
                for x in parts[2:]:
                    try:
                        numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                    except:
                        numeric_vals.append(0.0)
                if fc_read_header:
                    for i, iface in enumerate(fc_read_header):
                        val = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        fc_by_tag.setdefault(tag, {})
                        fc_by_tag[tag][f"{iface}-read"] = val
            if key == 'FCWRITE' and len(parts) > 2 and parts[1].startswith('T'):
                tag = parts[1]
                numeric_vals = []
                for x in parts[2:]:
                    try:
                        numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                    except:
                        numeric_vals.append(0.0)
                if fc_write_header:
                    for i, iface in enumerate(fc_write_header):
                        val = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        fc_by_tag.setdefault(tag, {})
                        fc_by_tag[tag][f"{iface}-write"] = val

    net_size_by_tag = {}
    with open(nmon_file, 'r', encoding='utf-8' , errors='ignore') as f3:
        for line in f3:
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            if parts[0] == 'NETSIZE':
                if len(parts) > 2 and not parts[1].startswith('T'):
                    continue
                if len(parts) > 2 and parts[1].startswith('T'):
                    tag = parts[1]
                    numeric_vals = []
                    for x in parts[2:]:
                        try:
                            numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                        except:
                            numeric_vals.append(0.0)
                    if len(numeric_vals) >= 4:
                        net_size_by_tag.setdefault(tag, {})
                        net_size_by_tag[tag]['en2-readsize']    = numeric_vals[0]
                        net_size_by_tag[tag]['lo0-readsize']    = numeric_vals[1]
                        net_size_by_tag[tag]['en2-writesize']   = numeric_vals[2]
                        net_size_by_tag[tag]['lo0-writesize']   = numeric_vals[3]

    
    # --- NEW: NETERROR (Network Errors & Collisions) ---
    net_error_by_tag = {}
    neterror_header = []
    with open(nmon_file, 'r', encoding='utf-8' , errors='ignore') as f_err:
        for line in f_err:
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            if parts[0] == 'NETERROR':
                # Header example:
                # NETERROR,Network Errors <Node>,en0-ierrs,lo0-ierrs,en0-oerrs,lo0-oerrs,en0-collisions,lo0-collisions
                if len(parts) > 2 and not parts[1].startswith('T'):
                    neterror_header = parts[2:]
                    continue
                # Data lines only if tag like Txxxx
                if len(parts) > 2 and parts[1].startswith('T') and neterror_header:
                    tag = parts[1]
                    numeric_vals = []
                    for x in parts[2:]:
                        try:
                            numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                        except:
                            numeric_vals.append(0.0)
                    d = {}
                    for i, col_name in enumerate(neterror_header):
                        d[col_name] = numeric_vals[i] if i < len(numeric_vals) else 0.0
                    net_error_by_tag[tag] = d
    fcxfer_by_tag = {}
    fcxfer_in_header = []
    fcxfer_out_header = []
    with open(nmon_file, 'r', encoding='utf-8' , errors='ignore') as f4:
        for line in f4:
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            key = parts[0]
            if key == 'FCXFERIN' and len(parts) > 2 and not parts[1].startswith('T'):
                fcxfer_in_header = parts[2:]
                continue
            if key == 'FCXFEROUT' and len(parts) > 2 and not parts[1].startswith('T'):
                fcxfer_out_header = parts[2:]
                continue
            if key == 'FCXFERIN' and len(parts) > 2 and parts[1].startswith('T'):
                tag = parts[1]
                numeric_vals = []
                for x in parts[2:]:
                    try:
                        numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                    except:
                        numeric_vals.append(0.0)
                if fcxfer_in_header:
                    for i, iface in enumerate(fcxfer_in_header):
                        val = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        fcxfer_by_tag.setdefault(tag, {})
                        fcxfer_by_tag[tag][f"{iface}-in"] = val
            if key == 'FCXFEROUT' and len(parts) > 2 and parts[1].startswith('T'):
                tag = parts[1]
                numeric_vals = []
                for x in parts[2:]:
                    try:
                        numeric_vals.append(float(x.strip()) if x.strip() else 0.0)
                    except:
                        numeric_vals.append(0.0)
                if fcxfer_out_header:
                    for i, iface in enumerate(fcxfer_out_header):
                        val = numeric_vals[i] if i < len(numeric_vals) else 0.0
                        fcxfer_by_tag.setdefault(tag, {})
                        fcxfer_by_tag[tag][f"{iface}-out"] = val

    # --- NEW: Extract fcs -> san_id mapping from BBBF lines (Port FC ID) ---
        fc_san_map = {}
        try:
            with open(nmon_file, 'r', encoding='utf-8', errors='ignore') as f_bbbf:
                for line in f_bbbf:
                    if not line.startswith('BBBF'):
                        continue
                    if 'Port FC ID' not in line:
                        continue
                    parts_line = [x.strip() for x in line.strip().split(',')]
                    fcs_name = None
                    for tok in parts_line:
                        if tok.startswith('fcs'):
                            fcs_name = tok
                            break
                    # take the last token as the hex id field when possible
                    last_field = parts_line[-1] if parts_line else ''
                    m = re.search(r'0x([0-9a-fA-F]{2})', last_field) or re.search(r'0x([0-9a-fA-F]{2})', line)
                    if fcs_name and m:
                        san_id = m.group(1).lower()
                        fc_san_map[fcs_name] = san_id
        except Exception as _e:
            # keep original logic intact; mapping is optional
            fc_san_map = {}
        all_docs = build_all_docs(
            cpu_data,
            lpar_data,
            proc_data,
            file_io_data,
            memnew_data_by_tag,
            zzzz_map,
            mem_data_by_tag,
            net_data_by_tag,
            netpacket_data_by_tag,
            diskread_data_by_tag,
            diskwrite_data_by_tag,
            diskbusy_data_by_tag,
            diskwait_data_by_tag,
            vgread_data_by_tag,
            vgwrite_data_by_tag,
            vgbusy_data_by_tag,
            vgsize_data_by_tag
        )

        for d in all_docs:
            tag_time = d["@timestamp"]
            the_tag = None
            for tkey, tval in zzzz_map.items():
                if tval == tag_time:
                    the_tag = tkey
                    break
            # NEW: add FC SAN map if available
            if fc_san_map:
                d["fc_san_map"] = fc_san_map
            if the_tag and the_tag in net_size_by_tag:
                d["netsize"] = net_size_by_tag[the_tag]
            if the_tag and the_tag in net_error_by_tag:
                d["neterror"] = net_error_by_tag[the_tag]

            if the_tag and the_tag in fc_by_tag:
                d["fc"] = fc_by_tag[the_tag]
            if the_tag and the_tag in fcxfer_by_tag:
                d["fcxfer"] = fcxfer_by_tag[the_tag]
            if the_tag and the_tag in jfsfile_data_by_tag:
                d["jfsfile"] = jfsfile_data_by_tag[the_tag]
            # NEW: add FS Cache Memory Use data if available
            if the_tag and the_tag in memuse_data_by_tag:
                d["memuse"] = memuse_data_by_tag[the_tag]
            # NEW: add paging data if available
            if the_tag and the_tag in page_data_by_tag:
                d["page"] = page_data_by_tag[the_tag]
            # NEW: add SEA data if available
            if the_tag and the_tag in sea_data_by_tag:
                d["sea"] = sea_data_by_tag[the_tag]
            # NEW: add SEA PHY data if available
            if the_tag and the_tag in seachphy_data_by_tag:
                d["seachphy"] = seachphy_data_by_tag[the_tag]
            # NEW: add SEA Packets/s data if available
            if the_tag and the_tag in seapacket_data_by_tag:
                d["seapacket"] = seapacket_data_by_tag[the_tag]
            # NEW: add MEM MB data if available
            if the_tag and the_tag in mem_mb_data_by_tag:
                d["mem_mb"] = mem_mb_data_by_tag[the_tag]
            # NEW: add CPU Use per logical CPU data if available (fixed)
            if the_tag and the_tag in cpu_use_data_by_tag:
                d["cpu_use"] = { cpu: {"user": rec["user_sum"] / rec["count"], "sys": rec["sys_sum"] / rec["count"]} 
                                 for cpu, rec in cpu_use_data_by_tag[the_tag].items() }
        top_docs = build_top_docs(top_data_by_tag, zzzz_map)

        # NEW: Create LPM docs
        lpm_docs = []
        for item in lpm_data:
            lpm_docs.append({
                '@timestamp': item['timestamp'],
                'state': item['state'],
                'old_serial': item['old_serial'],
                'current_serial': item['current_serial']
            })

        base_name = os.path.splitext(os.path.basename(nmon_file))[0]
        out_all_dir = os.path.join(output_dir, "all")
        os.makedirs(out_all_dir, exist_ok=True)
        all_path = os.path.join(out_all_dir, f"{base_name}_all.json")
        write_ndjson(all_docs, all_path)
        print(f"Wrote {len(all_docs)} docs => {all_path}")

        out_top_dir = os.path.join(output_dir, "top")
        os.makedirs(out_top_dir, exist_ok=True)
        top_path = os.path.join(out_top_dir, f"{base_name}_top.json")
        write_ndjson(top_docs, top_path)
        print(f"Wrote {len(top_docs)} top docs => {top_path}")

        return (node, frame, all_docs, top_docs, lpm_docs)

    ################################################################################
    # 6. main => parse => build => single HTML (16 + 5 = 21 charts total, plus new JFS, SEA, SEA Stacked, SEA Packets/s, MEM MB, Top PID charts)
    ################################################################################

def main():
    parser = argparse.ArgumentParser(
        description="Parse .nmon => NDJSON => single HTML with multiple charts (including FCXFER in/out, plus DISK/VG but no VG SIZE)."
    )
    parser.add_argument("--input_dir", type=str, required=True, help="Folder containing .nmon files")
    parser.add_argument("--output_dir", type=str, required=True, help="Output folder for NDJSON & HTML")
    parser.add_argument("--processes", type=int, default=cpu_count(), help="Number of processes to use")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    nmon_files = glob.glob(os.path.join(args.input_dir, "*.nmon"))
    if not nmon_files:
        print(f"No .nmon files found in {args.input_dir}")
        return

    lpar_data_map = {}
    top_data_map = {}
    lpm_data_map = {}
    frame_map = {}
    tasks = [(fp, args.output_dir) for fp in nmon_files]

    with Pool(processes=args.processes) as p:
        results = p.starmap(process_file, tasks)

    for (nodeName, frameName, all_docs, top_docs, lpm_docs) in results:
        if nodeName not in lpar_data_map:
            lpar_data_map[nodeName] = []
        if nodeName not in top_data_map:
            top_data_map[nodeName] = []
        if nodeName not in lpm_data_map:
            lpm_data_map[nodeName] = []
        frame_map[nodeName] = frameName
        lpar_data_map[nodeName].extend(all_docs)
        top_data_map[nodeName].extend(top_docs)
        lpm_data_map[nodeName].extend(lpm_docs)

    html_output = os.path.join(args.output_dir, "index.html")
    generate_html_page(lpar_data_map, top_data_map, frame_map, lpm_data_map, html_output)
    print("Completed. Open:", html_output)

if __name__ == "__main__":
    main()
