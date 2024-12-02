import requests
from typing import Dict
from airflow.models import Variable

PROFILE_FIELD_MAPPING = {
    "photoUrl": "imgUrl",
    "headline": "headline",
    "location": "location",
    "linkedInUrl": "linkedinProfile",
    "firstName": "First Name",
    "lastName": "Last Name",
    "summary": "description",
    "followerCount": "subscribers",
    "positions.positionHistory[0].title": "jobTitle",
    "positions.positionHistory[1].title": "jobTitle2",
    "positions.positionHistory[0].companyName": "company",
    "positions.positionHistory[1].companyName": "company2",
    "positions.positionHistory[0].linkedInUrl": "companyUrl",
    "positions.positionHistory[1].linkedInUrl": "companyUrl2",
    "positions.positionHistory[0].description": "jobDescription",
    "positions.positionHistory[1].description": "jobDescription2",
    "schools.educationHistory[0].schoolName": "school",
    "schools.educationHistory[1].schoolName": "school2",
    "schools.educationHistory[0].linkedInUrl": "schoolUrl",
    "schools.educationHistory[1].linkedInUrl": "schoolUrl2",
    "skills[0]": "skill1",
    "skills[1]": "skill2",
    "skills[2]": "skill3",
    "skills[3]": "skill4",
    "skills[4]": "skill5",
    "skills[5]": "skill6",
}


ACTIVITIES_FIELD_MAPPING = {
    "posts[0].activityUrl": "postUrl",
    "posts[0].authorPublicIdentifier": "profileUrl",
    "posts[0].text": "postContent",
    "posts[0].activityDate": "postDate",
    "reactions[0].type": "typeField",
    "reactions[0].relatedPost.activityUrl": "sharedPostUrl",
    "reactions[0].relatedPost.authorPublicIdentifier": "sharedPostProfileUrl",
    "reactions[0].relatedPost.text": "postContent",
    "reactions[0].relatedPost.activityDate": "postDate",
}

COMPANY_FIELD_MAPPING = {
    "industry" : "Industry",
    "specialities[0]": "specialities1",
    "specialities[1]": "specialities2",
    "specialities[2]": "specialities3",
    "specialities[3]": "specialities4",
    "specialities[4]": "specialities5",
    "specialities[5]": "specialities6",
    "followerCount": "follower"
}

scrapin_api_key = Variable.get("SCRAPIN_API_KEY", default_var="you_default_secret_key")

def extract_nested_value(data: Dict, path: str) -> any:
    keys = path.split(".")
    for key in keys:
        if "[" in key and "]" in key:
            base_key, index = key.split("[")
            index = int(index.strip("]"))
            data = data.get(base_key, [])
            if len(data) > index:
                data = data[index]
            else:
                return None
        else:
            data = data.get(key)
        if data is None:
            return None
    return data

def map_fields(data: Dict, field_mapping: Dict) -> Dict:
    mapped_data = {}
    for source_field, target_field in field_mapping.items():
        value = extract_nested_value(data, source_field)
        if value is not None:
            mapped_data[target_field] = value
    return mapped_data

def search_linkedin_profile(linkedInUrl: str = "https://www.linkedin.com/in/williamhgates") -> Dict:
    try:
        # url = "https://api.scrapin.io/enrichment/profile"
        # querystring = {"apikey":scrapin_api_key,"linkedInUrl":linkedInUrl}
        # response = requests.request("GET", url, params=querystring)
        # return map_fields(response.json()["person"], PROFILE_FIELD_MAPPING)
        data = {
            "publicIdentifier": "williamhgates",
            "memberIdentifier": "251749025",
            "linkedInIdentifier": "ACoAAA8BYqEBCGLg_vT_ca6mMEqkpp9nVffJ3hc",
            "linkedInUrl": "https://www.linkedin.com/in/williamhgates",
            "firstName": "Bill",
            "lastName": "Gates",
            "headline": "Co-chair, Bill & Melinda Gates Foundation",
            "location": "Seattle, Washington, United States of America",
            "summary": "Co-chair of the Bill & Melinda Gates Foundation. Founder of Breakthrough Energy. Co-founder of Microsoft. Voracious reader. Avid traveler. Active blogger.",
            "photoUrl": "https://media.licdn.com/dms/image/D5603AQHv6LsdiUg1kw/profile-displayphoto-shrink_800_800/0/1695167344576?e=1723680000&v=beta&t=NcEpysYwCpQ_NBg0QTz_a265pEOhfGICFJUX92-KNpw",
            "backgroundUrl": "https://media.licdn.com/dms/image/v2/D5616AQHy2R5tyt2YUA/profile-displaybackgroundimage-shrink_350_1400/profile-displaybackgroundimage-shrink_350_1400/0/1672782785474?e=1731542400&v=beta&t=0HWY04xeK0kEQngJOje3yNudJ6eTvNz2Q2HrTRB4hO8",
            "openToWork": False,
            "premium": False,
            "creationDate": {
            "month": 5,
            "year": 2013
            },
            "followerCount": 35415,
            "positions": {
            "positionsCount": 3,
            "positionHistory": [
                {
                "title": "Co-chair",
                "companyName": "Bill & Melinda Gates Foundation",
                "description": "",
                "startEndDate": {
                    "start": {
                    "month": 1,
                    "year": 2000
                    },
                    "end": ""
                },
                "companyLogo": "https://media.licdn.com/dms/image/C4E0BAQE7Na_mKQhIJg/company-logo_400_400/0/1633731811337/bill__melinda_gates_foundation_logo?e=1726099200&v=beta&t=LIgstVg1oR5LmBl9u1kolb_xeOqs5kX1ZTcUpaEtsE4",
                "linkedInUrl": "https://www.linkedin.com/company/8736/",
                "linkedInId": "8736"
                },
                {
                "title": "Founder",
                "companyName": "Breakthrough Energy",
                "description": "",
                "startEndDate": {
                    "start": {
                    "month": 1,
                    "year": 2015
                    },
                    "end": ""
                },
                "companyLogo": "https://media.licdn.com/dms/image/C4D0BAQGwD9vNu044FA/company-logo_400_400/0/1630531940051/breakthrough_energy_ventures_logo?e=1726099200&v=beta&t=DIU32ElAkeY4aqcq_9uJTAhiZI-v0GoOX77409cLZRE",
                "linkedInUrl": "https://www.linkedin.com/company/19141006/",
                "linkedInId": "19141006"
                },
                {
                "title": "Co-founder",
                "companyName": "Microsoft",
                "description": "",
                "startEndDate": {
                    "start": {
                    "month": 1,
                    "year": 1975
                    },
                    "end": ""
                },
                "companyLogo": "https://media.licdn.com/dms/image/C560BAQE88xCsONDULQ/company-logo_400_400/0/1630652622688/microsoft_logo?e=1726099200&v=beta&t=zueWlWXcJ4WjwGSzlUWgPOnjoAm8C2KfSIcWWHxWrGg",
                "linkedInUrl": "https://www.linkedin.com/company/1035/",
                "linkedInId": "1035"
                }
            ]
            },
            "schools": {
            "educationsCount": 2,
            "educationHistory": [
                {
                "degreeName": "",
                "fieldOfStudy": "",
                "description": "",
                "linkedInUrl": "https://www.linkedin.com/company/1646/",
                "schoolLogo": "https://media.licdn.com/dms/image/C4E0BAQF5t62bcL0e9g/company-logo_400_400/0/1631318058235?e=1726099200&v=beta&t=tSGQKfAlig70DD9n2_xkYR54yBTf7K3aKsau8PMQSVM",
                "schoolName": "Harvard University",
                "startEndDate": {
                    "start": {
                    "month": 1,
                    "year": 1973
                    },
                    "end": {
                    "month": 1,
                    "year": 1975
                    }
                }
                },
                {
                "degreeName": "",
                "fieldOfStudy": "",
                "description": "",
                "linkedInUrl": "https://www.linkedin.com/company/30288/",
                "schoolLogo": "https://media.licdn.com/dms/image/D560BAQGFmOQmzpxg9A/company-logo_400_400/0/1683732883164/lakeside_school_logo?e=1726099200&v=beta&t=cylwvrQe7Q4N8oU1hotzPfrae8yxPuzdtG1ocBSuEmA",
                "schoolName": "Lakeside School",
                "startEndDate": {
                    "start": {
                    "month": "",
                    "year": ""
                    },
                    "end": {
                    "month": "",
                    "year": ""
                    }
                }
                }
            ]
            },
            "skills": [],
            "languages": []
        }

        return map_fields(data, PROFILE_FIELD_MAPPING)

    except Exception as e:
        print(f"Err scraping {linkedInUrl} : {e}")
        return None

def search_linkedin_activity(linkedInUrl: str = "https://www.linkedin.com/in/williamhgates") -> Dict:
    try:
        # url = "https://api.scrapin.io/enrichment/activities"
        # querystring = {"apikey":scrapin_api_key,"linkedInUrl":linkedInUrl}
        # response = requests.request("GET", url, params=querystring)
        # return map_fields(response.json(),ACTIVITIES_FIELD_MAPPING)
        data = {
            "success": True,
            "posts": [
                {
                "id": "7254912034612379648",
                "text": "I'm proud that the Gates Foundation is opening a new office in Dakar, Senegal. Africa's development progress is a priority for the foundation, and I'm grateful for all our partners and staff who have worked so hard to make this happen.",
                "likesCount": 2449,
                "commentsCount": 372,
                "activityDate": "2024-10-23T17:50:18.799Z",
                "authorId": "ACoAAA8BYqEBCGLg_vT_ca6mMEqkpp9nVffJ3hc",
                "authorPublicIdentifier": "williamhgates",
                "authorName": "Bill Gates",
                "activityUrl": "https://www.linkedin.com/feed/update/urn:li:activity:7254912034612379648",
                "isRepublishedPost": True,
                "relatedPost": {
                    "text": "We recently celebrated a momentous occasion with the official launch of the Bill & Melinda Gates Foundation's Senegal office! This significant step underscores our ongoing dedication to Africa's development goals and our commitment to collaborating with local partners.\n\nThe launch isn't just about opening doors—it's about forging pathways to innovative solutions that will save millions of lives across the continent. \n\nIt was great to have you with us Chris Elias and Paulin BASINGA and thanks for moderating a great discussion Raïssa OKOÏ !\n\nAs we move forward, we continue to learn, co-create, and deepen our relationships with key stakeholders, driving meaningful change for communities in Senegal and beyond.\n\n#BMGFSenegalLaunch #AfricaDevelopment #PartnershipForImpact",
                    "activityDate": "2024-10-22T13:09:42.578Z",
                    "authorId": "104421531",
                    "authorPublicIdentifier": "gates-foundation-africa",
                    "authorName": "Gates Foundation Africa",
                    "activityUrl": "https://www.linkedin.com/feed/update/urn:li:activity:7254479030517698560"
                }
                }
            ],
            "comments": [
                {
                "text": "Thanks for having me. It's inspiring to see how far research for Alzheimer's has come.",
                "likesCount": 38,
                "commentsCount": 19,
                "activityDate": "2024-08-29T16:25:17.386Z",
                "authorId": "ACoAAA8BYqEBCGLg_vT_ca6mMEqkpp9nVffJ3hc",
                "authorName": "Bill Gates",
                "authorPublicIdentifier": "williamhgates",
                "post": {
                    "text": "This afternoon, we welcomed Bill Gates as he toured labs at the Indiana University School of Medicine and met with key faculty members to learn more about the expansive Alzheimer's disease research happening right here in Indianapolis.",
                    "likesCount": 3257,
                    "commentsCount": 118,
                    "activityDate": "2024-08-29T16:25:17.386Z",
                    "authorId": "3323",
                    "authorPublicIdentifier": "indiana-university-school-of-medicine",
                    "authorName": "Indiana University School of Medicine",
                    "activityUrl": "https://www.linkedin.com/posts/indiana-university-school-of-medicine_this-afternoon-we-welcomed-bill-gates-as-ugcPost-7231813250051383296-24DS?utm_source=combined_share_message&utm_medium=member_desktop_web",
                    "relatedPost": ""
                },
                "activityUrl": "https://www.linkedin.com/feed/update/urn:li:ugcPost:7231813250051383296?commentUrn=urn%3Ali%3Acomment%3A%28ugcPost%3A7231813250051383296%2C7234959305127444481%29&dashCommentUrn=urn%3Ali%3Afsd_comment%3A%287234959305127444481%2Curn%3Ali%3AugcPost%3A7231813250051383296%29"
                }
            ],
            "reactions": [
                {
                "type": "LIKE",
                "authorId": "ACoAAA8BYqEBCGLg_vT_ca6mMEqkpp9nVffJ3hc",
                "authorName": "Bill Gates",
                "authorPublicIdentifier": "williamhgates",
                "relatedComment": "",
                "relatedPost": {
                    "text": "What an incredible week at Climate Week NYC! Climate Week brings together an ecosystem of people and organizations committed to building a sustainable future. I felt a sense of urgency combined with a strong commitment to change and getting the work done.\n\nOne highlight was my conversation with Bill Gates at an event hosted by Breakthrough Energy and CBRE focused on the Clean Industrial Revolution and the critical role of emerging climate technologies. Our partnership with Breakthrough Energy to decarbonize buildings, highlighted by our work with Luxwall, is a significant step forward in our net-zero commitment. \n\nWe further demonstrated the opportunity to decarbonize the built environment by honoring our work with the Javits Center. The Javits Centers' innovative microgrid featuring the largest rooftop solar array and battery storage system in Manhattan is an inspiring example of the solutions available now that can be scaled to any built environment. \n\nAnother memorable moment was speaking at Carnegie Hall during the Siemens Arts Program and Atlantik-Brücke e.V. event. It was an honor to reflect on the power of technology and culture to change the world, and to celebrate the enduring German-American friendship that has been pivotal in our journey. \n\nAs we move forward, the message is clear: partnerships are essential. No company can do this alone. The ecosystem of partners we have at #ClimateWeekNYC is crucial to delivering impact with greater speed and scale. \n\nThank you to everyone who made this week possible. Let's continue to push boundaries and take bold actions together. The future is ours to shape.  \n\n#Sustainability #NetZero #Innovation #CleanEnergy",
                    "likesCount": 341,
                    "commentsCount": 13,
                    "activityDate": "2024-10-02T17:33:47.405Z",
                    "authorId": "ACoAAARuJH4BUfWDnLosZmlFyNr6SrDaRFmmBK4",
                    "authorPublicIdentifier": "barbara-humpton",
                    "authorName": "Barbara Humpton",
                    "activityUrl": "https://www.linkedin.com/posts/barbara-humpton_climateweeknyc-sustainability-netzero-ugcPost-7245556119614496768-Sufh?utm_source=combined_share_message&utm_medium=member_desktop_web",
                    "relatedPost": ""
                }
                }
            ],
            "credits_left": 15,
            "rate_limit_left": 19
        }
        return map_fields(data, ACTIVITIES_FIELD_MAPPING)

    except Exception as e:
        print(f"Err scraping {linkedInUrl} : {e}")
        return None

def search_linkedin_company(linkedInUrl: str = "https://www.linkedin.com/company/1035") -> Dict:
    try:
        # url = "https://api.scrapin.io/enrichment/company"
        # querystring = {"apikey":scrapin_api_key,"linkedInUrl":linkedInUrl}
        # response = requests.request("GET", url, params=querystring)
        # return map_fields(response.json()["company"], COMPANY_FIELD_MAPPING)
        data = {
            "linkedInId": "1035",
            "name": "Microsoft",
            "universalName": "microsoft",
            "linkedInUrl": "https://www.linkedin.com/company/1035",
            "employeeCount": 228581,
            "employeeCountRange": {
            "start": 10001,
            "end": 1
            },
            "websiteUrl": "https://news.microsoft.com/",
            "tagline": "",
            "description": "Every company has a mission. What's ours? To empower every person and every organization to achieve more. We believe technology can and should be a force for good and that meaningful innovation contributes to a brighter world in the future and today. Our culture doesn't just encourage curiosity; it embraces it. Each day we make progress together by showing up as our authentic selves. We show up with a learn-it-all mentality. We show up cheering on others, knowing their success doesn't diminish our own. We show up every day open to learning our own biases, changing our behavior, and inviting in differences. Because impact matters.\n\nMicrosoft operates in 190 countries and is made up of more than 220,000 passionate employees worldwide.\n",
            "industry": "Software Development",
            "phone": "",
            "specialities": [
            "Business Software",
            "Developer Tools",
            "Home & Educational Software",
            "Tablets",
            "Search",
            "Advertising",
            "Servers",
            "Windows Operating System",
            "Windows Applications & Platforms",
            "Smartphones",
            "Cloud Computing",
            "Quantum Computing",
            "Future of Work",
            "Productivity",
            "AI",
            "Artificial Intelligence",
            "Machine Learning",
            "Laptops",
            "Mixed Reality",
            "Virtual Reality",
            "Gaming",
            "Developers",
            "IT Professional"
            ],
            "followerCount": 22736947,
            "headquarter": {
            "city": "Redmond",
            "country": "US",
            "postalCode": "98052",
            "geographicArea": "Washington",
            "street1": "1 Microsoft Way",
            "street2": ""
            },
            "logo": "https://media.licdn.com/dms/image/C560BAQE88xCsONDULQ/company-logo_400_400/0/1630652622688/microsoft_logo?e=1725494400&v=beta&t=joSXHhDAEare7f9gk8MwXr2sOr84zX7HDx2h5znXEYI"
        }

        return map_fields(data, COMPANY_FIELD_MAPPING)
    except Exception as e:
        print(f"Err scraping {linkedInUrl} : {e}")
        return None