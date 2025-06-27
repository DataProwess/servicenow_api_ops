import requests

url = "https://lendlease.service-now.com/api/now/attachment/61a7faf3133cef848f1a3d27d144b04d/file"

payload = {}
headers = {
  'Authorization': 'Bearer k-Zqfljmt_2ZoWBMh_ifARZei4cazm1Ow43_BNi3J4zedYHSnBV331d1WIbbngspHV3ach7nvkGO3LllXiFwQg',
  'Cookie': 'BIGipServerpool_lendlease=d0eb094de8ff43f5cc2be299bfd8eec0; JSESSIONID=DA38BF46F30AA53F02E11236636B59BD; glide_node_id_for_js=efe2bd7e650020ffb1a3817795c1b2947f8fae623442f4932f54498ae9e12757; glide_session_store=FDBA964B3BD626507532D03EB3E45A22; glide_user_activity=U0N2M18xOjZsQWQvN3BKdUp5K0twRFhOOWFyMlhpYTdNSWN3VjFpc2lxbG9oRkk3blk9OjBtY3FmYmNBb2F0SXdZQ09DT2VYcEIxN2M0eUJ4ZUVEdnFxTmZDd1lkeFU9; glide_user_route=glide.f918fa4ee5b30c271260985b8b448be6'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
