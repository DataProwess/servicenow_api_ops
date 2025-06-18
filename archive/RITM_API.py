import requests
import json

url = "https://lendlease.service-now.com/api/now/table/sc_req_item?sysparm_display_value=true&sysparm_view=Default%20view"

payload = {}
headers = {
    'Authorization': 'Bearer eahpmDg0GSwehwuCD6SY1OB9FNe5tUviXn6fWNZh3KYhDdVQyWZ2yHTl3eZNB2XYneGuvK_Nqq1_cbFxRvjZow',
    'Cookie': 'BIGipServerpool_lendlease=c5889ad29f701618e3baa37002034b82; JSESSIONID=DBC4D656177BC7C975FCDADDDC26544B; glide_node_id_for_js=fc4812175032dd94c0ff92cf846b17cf27f0dce0a6beb49e12e5c7bb0f48d836; glide_session_store=8C855FE92BF52290E412F41CD891BFE4; glide_user_route=glide.5a07cc0a1b859ed021434a69d48daaeb'
}

response = requests.get(url, headers=headers, data=payload)
print(response)

# Save the response to a JSON file
with open("response.json", "w", encoding="utf-8") as f:
    json.dump(response.json(), f, indent=2)

print("Response saved to response.json")
