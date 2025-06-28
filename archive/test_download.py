import requests

url = "https://lendlease.service-now.com/api/now/attachment/e9a7faf3133cef848f1a3d27d144b04d/file"

payload = {}
headers = {
  'Authorization': 'Bearer Hgax013pczMA4TrtBgBiERSAoak9Yj93I_z57o3tKUd5uZQXyYstmhaj6uFu1d54MtKaJ3h2wMb51FNx5UwT_A',
  'Cookie': 'BIGipServerpool_lendlease=d0eb094de8ff43f5cc2be299bfd8eec0; JSESSIONID=DB7FFA250AA33BF2BB6FC5AADAE1F9D5; glide_node_id_for_js=efe2bd7e650020ffb1a3817795c1b2947f8fae623442f4932f54498ae9e12757; glide_session_store=BD30FA473B5E26507532D03EB3E45AD0; glide_user_activity=U0N2M18xOnEyOFZORWgrdTZJWWRaUG11am5VMFJBSGtJTWVoZUNzeGo2NzdTREFRdGc9Ojd0N2NPZDJIblpmT3dqZmFpUERqWTZQNjRmYzZRMFRUYXMyVXlxNDJtQWs9; glide_user_route=glide.f918fa4ee5b30c271260985b8b448be6'
}

# Download the file with a default filename
response = requests.get(url, headers=headers, stream=True)
if response.status_code == 200:
    with open("image.jpg", "wb") as f:  # Default filename
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print("Attachment downloaded as 'image.jpg'")
else:
    print(f"Failed to download: {response.status_code}")
