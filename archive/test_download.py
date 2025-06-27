import requests

url = "https://lendlease.service-now.com/sys_attachment.do?sysparm_referring_url=tear_off&view=true&sys_id=61a7faf3133cef848f1a3d27d144b04d"
headers = {
    'Authorization': 'Bearer k-Zqfljmt_2ZoWBMh_ifARZei4cazm1Ow43_BNi3J4zedYHSnBV331d1WIbbngspHV3ach7nvkGO3LllXiFwQg',
    'Cookie': 'BIGipServerpool_lendlease=d0eb094de8ff43f5cc2be299bfd8eec0; JSESSIONID=DB7FFA250AA33BF2BB6FC5AADAE1F9D5; glide_node_id_for_js=efe2bd7e650020ffb1a3817795c1b2947f8fae623442f4932f54498ae9e12757; glide_session_store=BD30FA473B5E26507532D03EB3E45AD0; glide_user_activity=U0N2M18xOnEyOFZORWgrdTZJW1aUG11am5VMFJBSGtJTWVoZUNzeGo2NzdTREFRdGc9Ojd0N2NPZDJIblpmT3dqZmFpUERqWTZQNjRmYzZRMFRUYXMyVXlxNDJtQWs9; glide_user_route=glide.f918fa4ee5b30c271260985b8b448be6'
}

custom_file_name = "my_custom_filename.ext"  # Change the extension to match the actual file type

response = requests.get(url, headers=headers, stream=True)
response.raise_for_status()

with open(custom_file_name, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)

print(f"File downloaded successfully as {custom_file_name}")
