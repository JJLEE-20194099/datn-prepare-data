# import dependencies
import streamlit as st
from streamlit_lottie import st_lottie
import requests
from streamlit_option_menu import option_menu
import pandas as pd
import os
import requests
import json
import time
from matplotlib import pyplot as plt
import seaborn as sns

from chart_app import fig1, fig2, fig3, fig4, fig5, fig6

from dash import Dash, html, dcc, Input, Output, Patch, clientside_callback, callback

from utils import update_district_ward_street_util, update_street_util, update_ward_street_util

st.set_page_config(
    page_title = "BKPrice System",
    layout="wide",
    page_icon='data/logo.png',
    initial_sidebar_state="expanded"
)

with open( "data/style.css" ) as css:
    st.markdown( f'<style>{css.read()}</style>' , unsafe_allow_html= True)

def predict_price(body, lottie_container_1, lottie_container_2):
    url = "http://localhost:2001/predict-realestate-batch"

    payload = json.dumps({
    "requests": [
        {
        "id": "1",
        "url": "/predict-realestate",
        "method": "POST",
        "headers": {
            "x-token": "DEFI AI",
            "Content-Type": "application/json"
        },
        "body": body
        }
    ],
    "base_url": "http://localhost:2001"
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    lottie_container_1.empty()
    lottie_container_2.empty()

    return response.json()[0]


data = json.load(open('/home/long/airflow/dags/schema/expectations/address.json', 'r'))


left, middle, right = st.columns((1, 1, 7))
with left:
    st.image("./data/logo.png")
with middle:
    st.header(":green[𝐵𝒦𝒫𝓇𝒾𝒸𝑒 𝒮𝓎𝓈𝓉𝑒𝓂]")
    st.write(":orange[                 𝓘𝓶𝓹𝓻𝓸𝓿𝓮 𝓟𝓻𝓮𝓭𝓲𝓬𝓽𝓲𝓸𝓷 𝓠𝓾𝓪𝓵𝓲𝓽𝔂]")



# 1. upper navbar
selected = option_menu(
    menu_title = None,
    # options=["Home", "Predict", "Prediction history", "Our Insights", "Contact"],
    options=["𝓗𝓸𝓶𝓮", "𝓑𝓚𝓟𝓻𝓲𝓬𝓮 𝓟𝓻𝓮𝓭𝓲𝓬𝓽", "𝓑𝓚𝓟𝓻𝓲𝓬𝓮 𝓗𝓲𝓼𝓽𝓸𝓻𝔂", "𝓑𝓚𝓟𝓻𝓲𝓬𝓮 𝓘𝓷𝓼𝓲𝓰𝓱𝓽𝓼", "𝓑𝓚𝓟𝓻𝓲𝓬𝓮 𝓒𝓱𝓪𝓽𝓫𝓸𝓽"],
    icons=["house", "bar-chart-line", "box", "file-bar-graph", "chat", "gear"],
    orientation="horizontal",
    styles={
        "container": {"padding": "2px", "background-color": "#fafafa", "color": "orange", "border-radius": "0"},
        "icon": {"color": "orange", "font-size": "20px"},
        "nav-link": {"font-size": "20px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
        "nav-link-selected": {"background-color": "green"},
    }
)


url = requests.get("https://assets9.lottiefiles.com/packages/lf20_18QlHa.json")
url_json = dict()
if url.status_code == 200:
    url_json = url.json()
else:
    print("Error in URL")
# 2. Home Page
if selected == "𝓗𝓸𝓶𝓮":
    outer_col1, outer_col2, outer_col3 = st.columns([1, 5, 1])
    with outer_col2:
        with st.container():
            col1, col2 = st.columns([2.5, 2.5])
            with col1:
                st.header(":green[𝓑𝓚𝓟𝓻𝓲𝓬𝓮 𝓔𝔁𝓹𝓵𝓸𝓻𝓮]")
                st.markdown("- :orange[Automate] data collection  \n- :orange[Automate] the process of building training dataset  \n- :orange[Automate] the process of updating and evaluating AI models \n- :orange[High level] of system automation\n- :orange[Leverage] the strengths of :orange[LLM] to interact with :orange[MLOps pipeline more easily]")
            with col2:
                st.header(":green[𝓐𝓫𝓸𝓾𝓽 𝓽𝓱𝓲𝓼 𝓪𝓹𝓹]")
                st.markdown("- Easily predict Vietnamese realestate price using our :orange[BKPrice Predictor]  \n- View realestate statistics using using our :orange[BKPrice Insights]  \n- View recent predictions using our :orange[BKPrice History] \n- Interact with BKPrice MLOps pipeline using :orange[BKPrice Chatbot]")

            st_lottie(url_json,
            reverse=False,
            height=20,
            width=1200,
            speed=1.25,
            loop=True,
            quality='low',
            key='high'
            )



        with st.container():
            col1, col2= st.columns([2.5, 2.5])
            with col1:
                s = f"<p style='font-size:22px; color: green; font-weight:600'>𝓦𝓱𝓪𝓽 𝓲𝓼 𝓜𝓛𝓞𝓹𝓼 𝓟𝓲𝓹𝓮𝓵𝓲𝓷𝓮?</p>"
                st.markdown(s, unsafe_allow_html=True)
                st.write("An MLOps pipeline is essentially an automated workflow that manages the entire lifecycle of a machine learning model, from its development to deployment and monitoring in production. This pipeline automates steps like data preparation, training, testing, and deployment, ensuring a smooth and efficient process for getting the most out of your models. By automating these tasks, MLOps pipelines help to improve the accuracy, reliability, and overall effectiveness of machine learning projects. To address many challenges in automatic tasks relate to vietnamese realestate, this study :orange[proposes an automated process] encompassing data preparation, model development, post-processing, automatic evaluation, and monitoring.")
            with col2:
                s = f"<p style='font-size:22px; color: green; font-weight:600'>𝓦𝓱𝔂 𝓹𝓻𝓮𝓭𝓲𝓬𝓽 𝓥𝓲𝓮𝓽𝓷𝓪𝓶𝓮𝓼𝓮 𝓻𝓮𝓪𝓵𝓮𝓼𝓽𝓪𝓽𝓮 𝓹𝓻𝓲𝓬𝓮?</p>"
                st.markdown(s, unsafe_allow_html=True)
                st.write("Vietnamese realestate data is updated daily :orange[requires artificial intelligence services to be updated] accordingly. Many AI services in the real estate sector :orange[have not yet recognized] the importance of this issue and have :orange[affected the user experience] in terms of the reliability of real estate price predictions. In addition, the process of building and deploying AI models in many products :orange[does not ensure integrity and reliability] and directly affects end users such as: deployed models are subjectively evaluated by the model builder, the training process is not properly monitored. Therefore, it is inevitable that :orange[AI models give unreliable prediction results], directly affecting users.")

            st_lottie(url_json,
            reverse=False,
            height=20,
            width=1200,
            speed=1.25,
            loop=True,
            quality='low',
            key='div2'
            )


# 3. Prediction history page
column1, column2, column3 = st.columns([1.5, 3, 1.5])
with column2:
    if selected == "𝓑𝓚𝓟𝓻𝓲𝓬𝓮 𝓗𝓲𝓼𝓽𝓸𝓻𝔂":
        st.subheader(":green[𝓗𝓲𝓼𝓽𝓸𝓻𝓲𝓬𝓪𝓵 𝓞𝓾𝓽𝓬𝓸𝓶𝓮𝓼]")
        if os.path.isfile("./data/historical_data.csv"):
            historical_data = pd.read_csv("./data/historical_data.csv")

            for city in ["hn", "hcm"]:

                sub_data = historical_data[historical_data["city"] == city]

                if sub_data.shape[0] == 0:
                    continue
                mean_data = sub_data.groupby('district')["prediction"].mean().reset_index()
                mean_list = mean_data["prediction"].tolist()
                prefix = "Ha Noi City" if city == "hn" else "Ho Chi Minh City"
                y = {"District": mean_data['district'].tolist(), f"Mean Predict Price in {prefix}":mean_list}
                chart_data = pd.DataFrame.from_dict(y)
                st.bar_chart(data=chart_data, x="District", y=f"Mean Predict Price in {prefix}", height = 500)

            st.divider()
            st.subheader(":green[𝓗𝓲𝓼𝓽𝓸𝓻𝓲𝓬𝓪𝓵 𝓘𝓷𝓹𝓾𝓽]")

            st.dataframe(historical_data, use_container_width=False)
        else:
            st.write(":green[𝓝𝓸 𝓱𝓲𝓼𝓽𝓸𝓻𝓲𝓬𝓪𝓵 𝓭𝓪𝓽𝓪]")


# 4. Our insights page
if selected == "𝓑𝓚𝓟𝓻𝓲𝓬𝓮 𝓘𝓷𝓼𝓲𝓰𝓱𝓽𝓼":
    outer_col4, outer_col5, outer_col6 = st.columns([1, 1, 1])
    with outer_col4:
        st.header("**", anchor=False)
        with st.expander(""):
            st.metric(label="", value=8, delta=None, delta_color="inverse")
            st.write("")
    with outer_col5:
            st.header("**", anchor=False)
            with st.expander(""):
                st.metric(label="", value=5, delta=None, delta_color="inverse")
                st.write("")
    with outer_col6:
        st.header("**", anchor=False)
        with st.expander(""):
            st.metric(label="", value=0, delta=None, delta_color="inverse")
            st.write("")

    outer_col7, outer_col8 = st.columns([2, 2])
    with outer_col7:
        st.header("𝓑𝓚𝓟𝓻𝓲𝓬𝓮 𝓜𝓪𝓹", anchor=False)
        st.plotly_chart(fig1, theme = 'streamlit', height = 1500)

    with outer_col8:
        st.header("𝓣𝓪𝓻𝓰𝓮𝓽𝓟𝓻𝓲𝓬𝓮 𝓜𝓪𝓹", anchor=False)
        st.plotly_chart(fig2, theme = 'streamlit', height = 1500)

    left1, middle1, right1 = st.columns((1, 7, 1))
    with middle1:
        style = "<style>h2 {text-align: center; color: green}</style>"
        st.markdown(style, unsafe_allow_html=True)
        st.header("𝓡𝓮𝓪𝓵𝓮𝓼𝓽𝓪𝓽𝓮 𝓟𝓻𝓲𝓬𝓮 𝓢𝓽𝓪𝓽𝓲𝓼𝓽𝓲𝓬 𝓑𝔂 𝓓𝓲𝓼𝓽𝓻𝓲𝓬𝓽", anchor=False)
        st.plotly_chart(fig3, theme = 'streamlit', height = 1500, use_container_width = True)

    left2, middle2, right2 = st.columns((1, 7, 1))
    with middle1:
        style = "<style>h2 {text-align: center; color: green}</style>"
        st.markdown(style, unsafe_allow_html=True)
        st.header("𝓢𝓮𝓮 𝓶𝓸𝓻𝓮 - 𝓛𝓲𝓷𝓴", anchor=False)





# # 5. Contact page
# if selected == "Contact":
#     col1, col2, col3 = st.columns([1, 4, 1])
#     with col2:
#         st.subheader("We'd love to hear from you!")
#         st.write("You can find us at:")
#         st.markdown("-> amruthaasathiakumar@gmail.com")
#         st.markdown("-> https://www.linkedin.com/in/amruthaa1108/")
#         st.markdown("-> https://github.com/amruthaa08")




def update_district_ward_street():
    result = update_district_ward_street_util(st.session_state.city_val)
    district_choices = result['district']
    ward_choices = result['ward']
    street_choices = result['street']

    st.session_state['district'] = district_choices
    st.session_state['ward'] = ward_choices
    st.session_state['street'] = street_choices

load_url = requests.get("https://assets5.lottiefiles.com/packages/lf20_awP420Zf8l.json")
load_url_json = dict()
if load_url.status_code == 200:
    load_url_json = load_url.json()
else:
      print("Error in URL")

load_url = requests.get("https://assets2.lottiefiles.com/packages/lf20_mDnmhAgZkb.json")
load_url_json_v2 = dict()
if load_url.status_code == 200:
    load_url_json_v2 = load_url.json()
else:
      print("Error in URL")

# 6. Prediction page
if selected == "𝓑𝓚𝓟𝓻𝓲𝓬𝓮 𝓟𝓻𝓮𝓭𝓲𝓬𝓽":
    # load schema
    with open("./data/schema.json", "r") as f:
        schema = json.load(f)

    st.session_state['city'] = ['hn', 'hcm']
    st.session_state['district'] = data['hn']['district']
    st.session_state['ward'] = data['hn']['ward']
    st.session_state['street'] = data['hn']['street']

    # extract column orders
    column_order_in = list(schema["column_info"].keys())[:-1]
    column_order_out = list(schema["transformed_columns"]["transformed_columns"])

    # sidebar section
    st.sidebar.info("Update these features to estimate house price based on realestate information")

    with st.expander(":green[Help]"):
        st.write("Select values from the sidebar and click the :green[Predict] button to get your prediction.")

    with st.expander(":green[Feature Dictionary]"):
        st.caption("1. landSize - represents size of realestate\n  2. city - vietnamese supported cities: hn (ha Noi City), hcm (Ho Chi Minh City)\n  3. district - determines district in Vietnam\n  4. ward - represents ward in Vietnam\n 5. street - represents street in Vietnam")
        st.caption("5. numberOfFloors - number of floors\n  6. numberOfBathRooms - number of bathrooms\n  7. numberOfBedRooms - number of bedrooms\n  8. frontWidth - indicates the width of the front of the house\n  9. endWidth - indicates the width of the end of the house\n  10. frontRoadWidth - represents the width of the road in front of the house")
        st.caption("11. certificateOfLandUseRight - represents ownership of real estate or not\n  12. typeOfRealEstate - determintes type of realestate \n 13. facade - indicates how many facades realestate have\n 14. houseDirection - indicates house direction\n 15. facility_check_ok - determines fully furnished or not\n 16. narrow_alley - shows the depth of the property if located in an alley: 1(low level of depth and close to street level), 2(modereate level of depth), 3(low level of depth and far from street level)")
        st.caption("17. version - BKPrice realestate prediction service version ")


    # collect input features
    options = {}
    for column, column_properties in schema["column_info"].items():
        if column == "churn":
            pass
        elif column_properties["dtype"] == "int64" or column_properties["dtype"]=="float64":
            min_val, max_val = column_properties["values"]
            data_type = column_properties["dtype"]

            feature_mean = (min_val+max_val) / 2
            if data_type == "int64":
                feature_mean = int(feature_mean)

            options[column] = st.sidebar.slider(column, min_val, max_val, value=feature_mean)

        # create categorical select boxes
        elif column_properties["dtype"] == "object" or column_properties["dtype"] == "bool":

            if column in ['district', 'ward', 'street']:
                column_properties["values"] = data['hn'][column] + data['hcm'][column]
            if column == 'city':
                city_choices = st.session_state['city']
                options[column] = st.sidebar.selectbox(column, city_choices, key="city_val", on_change=update_district_ward_street)
            elif column == 'district':
                options[column] = st.sidebar.selectbox(column, update_district_ward_street_util(st.session_state.city_val)['district'],key="district_val")
            elif column == 'ward':
                options[column] = st.sidebar.selectbox(column, update_ward_street_util(st.session_state.city_val, st.session_state.district_val)['ward'], key="ward_val")
            elif column == 'street':
                options[column] = st.sidebar.selectbox(column, update_street_util(st.session_state.city_val, st.session_state.district_val, st.session_state.ward_val)['street'], key="street_val")
            else:
                options[column] = st.sidebar.selectbox(column, column_properties["values"])

    # mean evening minutes value
    # mean_eve_mins = 200.29

    # st.write(options)



    # make predictions
    if st.button("Predict"):
        # convert options to df
        scoring_data = pd.Series(options).to_frame().T
        scoring_data = scoring_data[column_order_in]

        # for column, column_properties in schema["column_info"].items():
        #     if column != "churn" and column!= "id":
        #         dtype = column_properties["dtype"]
        #         scoring_data[column] = scoring_data[column].astype(dtype)
        # scoring_data["id"] = 0
        # scoring_sample = transform_data(scoring_data, column_order_out, mean_eve_mins, onehot)
        # st.write(scoring_sample)

        lottie_container_1 = st.empty()
        with lottie_container_1:

            st_lottie(load_url_json_v2,
                speed=1.5,
                height = 200,
                quality='high',
                loop=True,
                key='Car'
            )

        lottie_container_2 = st.empty()
        with lottie_container_2:

            st_lottie(load_url_json,
                speed=1.25,
                height = 200,
                quality='high',
                loop=True,
                key='Boy'
            )

        estimate_price = predict_price(options, lottie_container_1, lottie_container_2)
        st.write("Predicted outcome")
        if estimate_price > 200:
            st.write(f"Estimate Price :green[{estimate_price}]")
        else:
            st.write(f"Estimate Price :green[{estimate_price}]")
        st.write("Provided Details")
        st.write(options)

        try:

            options["prediction"] = estimate_price

            df = pd.read_csv('./data/historical_data.csv')
            records = df.to_dict('records')
            records.append(options)

            df = pd.DataFrame(records)
            df = df.drop_duplicates(keep = 'first')

        except Exception as e:
            df = pd.DataFrame([options])

        df.to_csv("./data/historical_data.csv", header = True, index = False)


if selected == "𝓑𝓚𝓟𝓻𝓲𝓬𝓮 𝓒𝓱𝓪𝓽𝓫𝓸𝓽":
    gradio_interface_url = "http://127.0.0.1:7860/"  # Example URL
    st.write(f'<iframe src="{gradio_interface_url}"  width = "1750" height="600"></iframe>', unsafe_allow_html=True)
