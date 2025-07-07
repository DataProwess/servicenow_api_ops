import re
import webbrowser
import time

log_data = """
2025-07-05 16:52:29 - ERROR -    ✗ Failed to download attachment 'be9920791b7a4110ef4543b4bd4bcba5_attachment' ,for ticket= HRC0023140, table_sys_id= be9920791b7a4110ef4543b4bd4bcba5 , sys_id = 2f99ac791b7a4110ef4543b4bd4bcb69 , (Status 500)
2025-07-05 16:52:30 - ERROR -    ✗ Failed to download attachment 'be9920791b7a4110ef4543b4bd4bcba5_attachment' ,for ticket= HRC0023140, table_sys_id= be9920791b7a4110ef4543b4bd4bcba5 , sys_id = 6399ac791b7a4110ef4543b4bd4bcb40 , (Status 500)
2025-07-05 16:52:31 - ERROR -    ✗ Failed to download attachment 'be9920791b7a4110ef4543b4bd4bcba5_attachment' ,for ticket= HRC0023140, table_sys_id= be9920791b7a4110ef4543b4bd4bcba5 , sys_id = ab99ac791b7a4110ef4543b4bd4bcb67 , (Status 500)
2025-07-05 16:52:32 - ERROR -    ✗ Failed to download attachment 'be9920791b7a4110ef4543b4bd4bcba5_attachment' ,for ticket= HRC0023140, table_sys_id= be9920791b7a4110ef4543b4bd4bcba5 , sys_id = e399ac791b7a4110ef4543b4bd4bcb3d , (Status 500)
"""

# Extract only the real attachment sys_id
sys_ids = re.findall(r'(?<!table_)sys_id\s*=\s*([a-f0-9]{32})', log_data)

# Remove duplicates and sort
unique_sys_ids = sorted(set(sys_ids))

# URL template (using a fixed table sys_id or none if you want)
# Replace the sys_id inside sysparm_this_url= if needed
for sid in unique_sys_ids:
    url = (
        f"https://lendlease.service-now.com/sys_attachment.do?sys_id={sid}"
        f"&sysparm_this_url=sn_hr_core_case_workforce_admin.do"
    )
    print(f"Opening: {url}")
    webbrowser.open(url)
    time.sleep(2)
