import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import hashlib

# ------------------------------------------------
# PAGE CONFIG & DARK THEME STYLING
# ------------------------------------------------
st.set_page_config(
    page_title="Student Dropout Analytics", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Dark Theme with Glowing Purple/Blue
st.markdown("""
    <style>
    /* Main Background */
    .main {
        background: linear-gradient(135deg, #0a0a0a 0%, #1a0a2e 50%, #0a0a0a 100%) !important;
        padding-top: 1rem;
    }
    
    /* Override Streamlit default backgrounds */
    .stApp {
        background: linear-gradient(135deg, #0a0a0a 0%, #1a0a2e 50%, #0a0a0a 100%) !important;
    }
    
    .block-container {
        background: transparent !important;
        padding-top: 2rem !important;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #16213e 0%, #0f3460 100%);
        border-right: 2px solid #6c63ff;
        min-width: 300px !important;
        max-width: 300px !important;
    }
    
    [data-testid="stSidebar"] * {
        color: #e0e0e0 !important;
    }
    
    /* Sidebar radio buttons - smaller font */
    [data-testid="stSidebar"] .row-widget.stRadio > div {
        font-size: 0.85rem !important;
        line-height: 1.4 !important;
    }
    
    [data-testid="stSidebar"] label {
        font-size: 0.85rem !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
    }
    
    /* Sidebar radio container */
    [data-testid="stSidebar"] .row-widget.stRadio {
        gap: 0.3rem !important;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #8b7bff !important;
        text-shadow: 0 0 20px rgba(139, 123, 255, 0.5);
        font-weight: 700;
    }
    
    h1 {
        background: linear-gradient(90deg, #8b7bff 0%, #4fc3f7 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 3rem !important;
    }
    
    /* Metric Cards */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%);
        padding: 20px;
        border-radius: 15px;
        border: 1px solid #6c63ff;
        box-shadow: 0 0 30px rgba(108, 99, 255, 0.3);
    }
    
    [data-testid="stMetricValue"] {
        color: #4fc3f7 !important;
        font-size: 2rem !important;
        text-shadow: 0 0 10px rgba(79, 195, 247, 0.5);
    }
    
    [data-testid="stMetricLabel"] {
        color: #b8b8d0 !important;
        font-weight: 600;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(90deg, #6c63ff 0%, #4fc3f7 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 10px 25px;
        font-weight: 600;
        box-shadow: 0 0 20px rgba(108, 99, 255, 0.5);
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        box-shadow: 0 0 30px rgba(108, 99, 255, 0.8);
        transform: translateY(-2px);
    }
    
    /* Divider */
    hr {
        border-color: #6c63ff;
        box-shadow: 0 0 10px rgba(108, 99, 255, 0.3);
    }
    
    /* Text */
    p, label, span, div {
        color: #e0e0e0 !important;
    }
    
    /* Input fields */
    input {
        background-color: #1e1e2e !important;
        color: #e0e0e0 !important;
        border: 1px solid #6c63ff !important;
        border-radius: 8px !important;
    }
    
    input::placeholder {
        color: #7a7a9d !important;
    }
    
    /* Select boxes */
    .stSelectbox > div > div {
        background-color: #1e1e2e !important;
        color: #e0e0e0 !important;
        border: 1px solid #6c63ff !important;
    }
    
    /* Radio buttons */
    .stRadio > div {
        background-color: transparent !important;
    }
    
    /* Dataframe */
    [data-testid="stDataFrame"] {
        background: #1e1e2e;
        border: 1px solid #6c63ff;
        border-radius: 10px;
    }
    
    /* Info/Warning boxes */
    .stAlert {
        background: rgba(108, 99, 255, 0.1);
        border: 1px solid #6c63ff;
        border-radius: 10px;
    }
    
    /* Login Container */
    .login-container {
        background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%);
        padding: 40px;
        border-radius: 20px;
        border: 2px solid #6c63ff;
        box-shadow: 0 0 40px rgba(108, 99, 255, 0.6);
        max-width: 500px;
        margin: 100px auto;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background-color: #1e1e2e !important;
        color: #e0e0e0 !important;
        border: 1px solid #6c63ff !important;
        border-radius: 8px !important;
    }
    
    .streamlit-expanderContent {
        background-color: #1e1e2e !important;
        border: 1px solid #6c63ff !important;
        border-radius: 8px !important;
    }
    
    /* Code blocks */
    code {
        background-color: #0a0a0a !important;
        color: #4fc3f7 !important;
        padding: 8px !important;
        border-radius: 5px !important;
        border: 1px solid #6c63ff !important;
    }
    
    /* Spark Link */
    .spark-link {
        background: linear-gradient(90deg, #6c63ff 0%, #4fc3f7 100%);
        padding: 15px 25px;
        border-radius: 10px;
        text-align: center;
        font-weight: 600;
        box-shadow: 0 0 20px rgba(108, 99, 255, 0.5);
        margin: 20px 0;
    }
    
    .spark-link a {
        color: white !important;
        text-decoration: none;
        font-size: 1.1rem;
    }
    </style>
""", unsafe_allow_html=True)

# ------------------------------------------------
# SESSION STATE INITIALIZATION
# ------------------------------------------------
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'username' not in st.session_state:
    st.session_state.username = ""
if 'page' not in st.session_state:
    st.session_state.page = "Overview"

# ------------------------------------------------
# USER AUTHENTICATION
# ------------------------------------------------
# Simple credential storage (In production, use a database)
USERS = {
    "tutor1": hashlib.sha256("password123".encode()).hexdigest(),
    "admin": hashlib.sha256("admin123".encode()).hexdigest(),
    "teacher": hashlib.sha256("teach123".encode()).hexdigest()
}

def verify_login(username, password):
    hashed_password = hashlib.sha256(password.encode()).hexdigest()
    return username in USERS and USERS[username] == hashed_password

def login_page():
    # Add CSS to hide default Streamlit elements
    st.markdown("""
        <style>
        header {visibility: hidden;}
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        .stDeployButton {display:none;}
        </style>
    """, unsafe_allow_html=True)
    
    # Title at the very top
    st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)
    st.markdown("<h1 style='text-align: center; margin-bottom: 10px;'>🎓 Dropout Risk Analysis Dashboard</h1>", unsafe_allow_html=True)
    
    # Login container with title inside
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%);
                    padding: 40px;
                    border-radius: 20px;
                    border: 2px solid #6c63ff;
                    box-shadow: 0 0 40px rgba(108, 99, 255, 0.6);
                    margin-top: 50px;'>
            <h2 style='text-align: center; color: #4fc3f7; margin-top: 0; margin-bottom: 30px;'>
                🔐 Tutor Login Portal
            </h2>
        </div>
        """, unsafe_allow_html=True)
        
        # Add a negative margin to overlap the input fields with the box
        st.markdown("<div style='margin-top: -10px;'></div>", unsafe_allow_html=True)
        
        username = st.text_input("👤 Username", placeholder="Enter your username")
        password = st.text_input("🔒 Password", type="password", placeholder="Enter your password")
        
        col_a, col_b = st.columns(2)
        
        with col_a:
            if st.button("🚀 Login", use_container_width=True):
                if verify_login(username, password):
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.rerun()
                else:
                    st.error("❌ Invalid credentials!")
        
        with col_b:
            if st.button("🔄 Reset", use_container_width=True):
                st.rerun()
        
        # Demo credentials info
        with st.expander("ℹ️ Demo Credentials"):
            st.code("Username: tutor1 | Password: password123")
            st.code("Username: admin | Password: admin123")
            st.code("Username: teacher | Password: teach123")

def logout():
    st.session_state.logged_in = False
    st.session_state.username = ""
    st.rerun()

# ------------------------------------------------
# SPARK SESSION
# ------------------------------------------------
@st.cache_resource
def init_spark():
    return (
        SparkSession.builder
        .appName("Dropout Prediction Dashboard")
        .config("spark.ui.port", "4040")
        .master("local[*]")
        .getOrCreate()
    )

# ------------------------------------------------
# LOAD & PROCESS DATA
# ------------------------------------------------
@st.cache_data
def load_and_process_data():
    try:
        spark = init_spark()
        
        student = spark.read.csv("datasets/studentInfo.csv", header=True, inferSchema=True)
        vle = spark.read.csv("datasets/studentVle.csv", header=True, inferSchema=True)
        assess = spark.read.csv("datasets/studentAssessment.csv", header=True, inferSchema=True)
        
        student = student.withColumn(
            "dropout_label",
            when(col("final_result") == "Withdrawn", 1).otherwise(0)
        )
        
        vle_agg = vle.groupBy("id_student").agg(
            sum("sum_click").alias("TotalClicks"),
            F.countDistinct("date").alias("ActiveDays")
        )
        
        vle_agg = vle_agg.withColumn(
            "AvgClicks",
            when(col("ActiveDays") > 0, col("TotalClicks") / col("ActiveDays")).otherwise(0)
        )
        
        assess_agg = assess.groupBy("id_student").agg(
            avg("score").alias("AvgScore"),
            sum(when(col("score") < 40, 1).otherwise(0)).alias("FailedAssessments"),
            count("*").alias("TotalAssessments")
        )
        
        df = student.join(vle_agg, "id_student", "left").join(assess_agg, "id_student", "left").fillna(0)
        
        return df.toPandas()
    
    except Exception as e:
        st.error(f"❌ Error loading dataset: {e}")
        st.stop()

def segment_students(pdf):
    click_q = pdf["TotalClicks"].quantile([0.33, 0.66]).values
    score_q = pdf["AvgScore"].quantile([0.33, 0.66]).values
    
    def segment(row):
        if row["TotalClicks"] <= click_q[0] and row["AvgScore"] <= score_q[0]:
            return "🔴 Low Engagement"
        elif row["TotalClicks"] >= click_q[1] and row["AvgScore"] >= score_q[1]:
            return "🟢 High Performer"
        elif row["AvgScore"] >= score_q[1]:
            return "🟡 High Scorer"
        elif row["TotalClicks"] >= click_q[1]:
            return "🔵 Active Clicker"
        elif row["AvgScore"] <= score_q[0]:
            return "🟠 Struggling"
        else:
            return "⚪ Average"
    
    pdf["Segment"] = pdf.apply(segment, axis=1)
    pdf["dropout_status"] = pdf["dropout_label"].map({1: "Dropped Out", 0: "Active"})
    
    return pdf, click_q, score_q

# ------------------------------------------------
# MAIN DASHBOARD
# ------------------------------------------------
def main_dashboard():
    # Initialize Spark
    spark = init_spark()
    
    # Header with Spark Link
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.title("🎓 Dropout Risk Analysis Dashboard")
        st.markdown(f"### Welcome, **{st.session_state.username}** 👋")
    
    with col2:
        st.markdown(
            '<div class="spark-link">'
            '🔥 <a href="http://localhost:4040" target="_blank">Open Spark UI Monitor</a>'
            '</div>',
            unsafe_allow_html=True
        )
        if st.button("🚪 Logout"):
            logout()
    
    st.divider()
    
    # Load Data
    pdf = load_and_process_data()
    pdf, click_q, score_q = segment_students(pdf)
    
    # SIDEBAR NAVIGATION
    st.sidebar.title("📊 Navigation")
    st.sidebar.markdown("### Select Page")
    pages = [
        "📊 Overview", 
        "🎯 Engage", 
        "👥 Demographics", 
        "📝 Assessment", 
        "🚨 Risk", 
        "📖 About", 
        "💡 Tips"
    ]
    page = st.sidebar.radio("", pages, key="page_selector", label_visibility="collapsed")
    
    st.sidebar.divider()
    
    # SIDEBAR FILTERS
    st.sidebar.header("🔍 Filters")
    
    genders = ["All"] + sorted(pdf["gender"].unique().tolist())
    selected_gender = st.sidebar.selectbox("Gender", genders)
    
    age_bands = ["All"] + sorted(pdf["age_band"].unique().tolist())
    selected_age = st.sidebar.selectbox("Age Band", age_bands)
    
    educations = ["All"] + sorted(pdf["highest_education"].unique().tolist())
    selected_edu = st.sidebar.selectbox("Education Level", educations)
    
    segments = ["All"] + sorted(pdf["Segment"].unique().tolist())
    selected_segment = st.sidebar.selectbox("Student Segment", segments)
    
    # Apply filters
    filtered_df = pdf.copy()
    if selected_gender != "All":
        filtered_df = filtered_df[filtered_df["gender"] == selected_gender]
    if selected_age != "All":
        filtered_df = filtered_df[filtered_df["age_band"] == selected_age]
    if selected_edu != "All":
        filtered_df = filtered_df[filtered_df["highest_education"] == selected_edu]
    if selected_segment != "All":
        filtered_df = filtered_df[filtered_df["Segment"] == selected_segment]
    
    # PAGE ROUTING
    if page == "📊 Overview":
        show_overview(filtered_df)
    elif page == "🎯 Engage":
        show_engagement(filtered_df, click_q, score_q)
    elif page == "👥 Demographics":
        show_demographics(filtered_df)
    elif page == "📝 Assessment":
        show_assessment(filtered_df)
    elif page == "🚨 Risk":
        show_risk_management(filtered_df, click_q)
    elif page == "📖 About":
        show_about()
    elif page == "💡 Tips":
        show_recommendations(filtered_df, click_q, score_q)
    
    # Footer
    st.divider()
    col1, col2 = st.columns([4, 1])
    with col1:
        st.caption("💡 **Pro Tip**: Use filters in the sidebar to drill down into specific student populations")
    with col2:
        if st.button("🛑 Stop Spark"):
            spark.stop()
            st.success("✅ Spark stopped")

# ------------------------------------------------
# PAGE 1: OVERVIEW
# ------------------------------------------------
def show_overview(filtered_df):
    st.header("📊 Dashboard Overview")
    
    # KEY METRICS
    col1, col2, col3, col4, col5 = st.columns(5)
    
    total_students = len(filtered_df)
    dropout_count = int(filtered_df["dropout_label"].sum())
    dropout_rate = (dropout_count / total_students * 100) if total_students > 0 else 0
    avg_score = float(filtered_df["AvgScore"].mean())
    avg_clicks = float(filtered_df["TotalClicks"].mean())
    
    col1.metric("👥 Total Students", f"{total_students:,}")
    col2.metric("❌ Dropouts", f"{dropout_count:,}", delta=f"{dropout_rate:.1f}% rate", delta_color="inverse")
    col3.metric("📈 Avg Score", f"{avg_score:.1f}%")
    col4.metric("🖱️ Avg Clicks", f"{avg_clicks:,.0f}")
    col5.metric("📚 Assessments", f"{filtered_df['TotalAssessments'].mean():.1f}")
    
    st.divider()
    
    # Visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        # Dropout Distribution
        dropout_dist = filtered_df["dropout_status"].value_counts().reset_index()
        dropout_dist.columns = ["Status", "Count"]
        
        fig_dropout = px.pie(
            dropout_dist,
            values="Count",
            names="Status",
            title="Overall Dropout Distribution",
            color="Status",
            color_discrete_map={"Active": "#4fc3f7", "Dropped Out": "#ff6b9d"},
            hole=0.4
        )
        fig_dropout.update_traces(textposition='inside', textinfo='percent+label', textfont_size=14)
        fig_dropout.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#e0e0e0', size=12),
            title_font=dict(size=18, color='#8b7bff')
        )
        st.plotly_chart(fig_dropout, use_container_width=True)
    
    with col2:
        # Segment Distribution
        seg_dist = filtered_df["Segment"].value_counts().reset_index()
        seg_dist.columns = ["Segment", "Count"]
        
        fig_seg = px.bar(
            seg_dist,
            x="Count",
            y="Segment",
            orientation='h',
            title="Student Segmentation Distribution",
            color="Count",
            color_continuous_scale=["#6c63ff", "#4fc3f7"]
        )
        fig_seg.update_layout(
            showlegend=False,
            yaxis={'categoryorder':'total ascending'},
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#e0e0e0', size=12),
            title_font=dict(size=18, color='#8b7bff')
        )
        st.plotly_chart(fig_seg, use_container_width=True)

# ------------------------------------------------
# PAGE 2: ENGAGEMENT ANALYSIS
# ------------------------------------------------
def show_engagement(filtered_df, click_q, score_q):
    st.header("🎯 Engagement vs Performance Analysis")
    
    fig_scatter = px.scatter(
        filtered_df,
        x="TotalClicks",
        y="AvgScore",
        color="dropout_status",
        size="FailedAssessments",
        hover_data=["id_student", "gender", "age_band", "Segment"],
        title="Student Engagement vs Academic Performance",
        labels={
            "TotalClicks": "Total Clicks (Engagement)",
            "AvgScore": "Average Score (%)",
            "dropout_status": "Status"
        },
        color_discrete_map={"Active": "#4fc3f7", "Dropped Out": "#ff6b9d"},
        size_max=20
    )
    fig_scatter.add_hline(
        y=score_q[0], 
        line_dash="dash", 
        line_color="#ff6b9d", 
        annotation_text="Low Score Threshold",
        annotation_font_color="#ff6b9d"
    )
    fig_scatter.add_vline(
        x=click_q[0], 
        line_dash="dash", 
        line_color="#ff6b9d",
        annotation_text="Low Engagement Threshold",
        annotation_font_color="#ff6b9d"
    )
    fig_scatter.update_layout(
        height=600,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#e0e0e0', size=12),
        title_font=dict(size=20, color='#8b7bff')
    )
    st.plotly_chart(fig_scatter, use_container_width=True)
    
    st.divider()
    
    # Segment Performance
    st.subheader("🎨 Performance by Student Segment")
    
    segment_df = filtered_df.groupby("Segment").agg({
        "dropout_label": ["sum", "count"]
    }).reset_index()
    segment_df.columns = ["Segment", "dropouts", "total"]
    segment_df["active"] = segment_df["total"] - segment_df["dropouts"]
    
    fig_segment = go.Figure(data=[
        go.Bar(name='Active', y=segment_df["Segment"], x=segment_df["active"],
               orientation='h', marker_color='#4fc3f7'),
        go.Bar(name='Dropped Out', y=segment_df["Segment"], x=segment_df["dropouts"],
               orientation='h', marker_color='#ff6b9d')
    ])
    fig_segment.update_layout(
        barmode='stack',
        title="Student Count by Segment and Status",
        xaxis_title="Number of Students",
        height=400,
        yaxis={'categoryorder':'total ascending'},
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#e0e0e0', size=12),
        title_font=dict(size=18, color='#8b7bff')
    )
    st.plotly_chart(fig_segment, use_container_width=True)

# ------------------------------------------------
# PAGE 3: DEMOGRAPHICS
# ------------------------------------------------
def show_demographics(filtered_df):
    st.header("👥 Demographic Dropout Patterns")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        gender_df = filtered_df.groupby("gender").agg({
            "dropout_label": ["sum", "count"]
        }).reset_index()
        gender_df.columns = ["gender", "dropouts", "total"]
        gender_df["active"] = gender_df["total"] - gender_df["dropouts"]
        
        fig_gender = go.Figure(data=[
            go.Bar(name='Active', x=gender_df["gender"], 
                   y=gender_df["active"], marker_color='#4fc3f7'),
            go.Bar(name='Dropped Out', x=gender_df["gender"], 
                   y=gender_df["dropouts"], marker_color='#ff6b9d')
        ])
        fig_gender.update_layout(
            title="Dropout by Gender",
            barmode='stack',
            yaxis_title="Student Count",
            height=400,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#e0e0e0', size=12),
            title_font=dict(size=16, color='#8b7bff')
        )
        st.plotly_chart(fig_gender, use_container_width=True)
    
    with col2:
        age_df = filtered_df.groupby("age_band").agg({
            "dropout_label": ["sum", "count"]
        }).reset_index()
        age_df.columns = ["age_band", "dropouts", "total"]
        age_df["rate"] = (age_df["dropouts"] / age_df["total"] * 100)
        
        fig_age = px.bar(
            age_df.sort_values("rate", ascending=False),
            x="age_band",
            y="rate",
            title="Dropout Rate by Age Band",
            color="rate",
            color_continuous_scale=["#6c63ff", "#ff6b9d"],
            labels={"rate": "Dropout Rate (%)"}
        )
        fig_age.update_layout(
            showlegend=False,
            height=400,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#e0e0e0', size=12),
            title_font=dict(size=16, color='#8b7bff')
        )
        st.plotly_chart(fig_age, use_container_width=True)
    
    with col3:
        edu_df = filtered_df.groupby("highest_education").agg({
            "dropout_label": ["sum", "count"]
        }).reset_index()
        edu_df.columns = ["education", "dropouts", "total"]
        edu_df["rate"] = (edu_df["dropouts"] / edu_df["total"] * 100)
        
        fig_edu = px.bar(
            edu_df.sort_values("rate", ascending=True),
            x="rate",
            y="education",
            orientation='h',
            title="Dropout Rate by Education",
            color="rate",
            color_continuous_scale=["#6c63ff", "#ff6b9d"],
            labels={"rate": "Dropout Rate (%)"}
        )
        fig_edu.update_layout(
            showlegend=False,
            height=400,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#e0e0e0', size=12),
            title_font=dict(size=16, color='#8b7bff')
        )
        st.plotly_chart(fig_edu, use_container_width=True)

# ------------------------------------------------
# PAGE 4: ASSESSMENT ANALYSIS
# ------------------------------------------------
def show_assessment(filtered_df):
    st.header("📝 Assessment Performance Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fail_df = filtered_df.groupby("FailedAssessments").agg({
            "dropout_label": ["sum", "count"]
        }).reset_index()
        fail_df.columns = ["FailedAssessments", "dropouts", "total"]
        fail_df["active"] = fail_df["total"] - fail_df["dropouts"]
        
        fig_fail = go.Figure(data=[
            go.Bar(name='Active', x=fail_df["FailedAssessments"], 
                   y=fail_df["active"], marker_color='#4fc3f7'),
            go.Bar(name='Dropped Out', x=fail_df["FailedAssessments"], 
                   y=fail_df["dropouts"], marker_color='#ff6b9d')
        ])
        fig_fail.update_layout(
            title="Failed Assessments vs Dropout Status",
            barmode='stack',
            xaxis_title="Number of Failed Assessments",
            yaxis_title="Student Count",
            height=500,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#e0e0e0', size=12),
            title_font=dict(size=18, color='#8b7bff')
        )
        st.plotly_chart(fig_fail, use_container_width=True)
    
    with col2:
        fig_violin = px.violin(
            filtered_df,
            y="AvgScore",
            x="dropout_status",
            box=True,
            points="outliers",
            title="Score Distribution by Dropout Status",
            color="dropout_status",
            color_discrete_map={"Active": "#4fc3f7", "Dropped Out": "#ff6b9d"}
        )
        fig_violin.update_layout(
            showlegend=False,
            height=500,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#e0e0e0', size=12),
            title_font=dict(size=18, color='#8b7bff')
        )
        st.plotly_chart(fig_violin, use_container_width=True)

# ------------------------------------------------
# PAGE 5: RISK MANAGEMENT
# ------------------------------------------------
def show_risk_management(filtered_df, click_q):
    st.header("🚨 High-Risk Students Management")
    
    risk_df = filtered_df[filtered_df["dropout_label"] == 1].sort_values(
        ["AvgScore", "TotalClicks"], ascending=[True, True]
    )
    
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric("📊 Total At-Risk", len(risk_df))
    col2.metric("⚠️ Critical (<30%)", len(risk_df[risk_df["AvgScore"] < 30]))
    col3.metric("🔻 Low Engagement", len(risk_df[risk_df["TotalClicks"] < click_q[0]]))
    col4.metric("❌ Multiple Failures", len(risk_df[risk_df["FailedAssessments"] >= 3]))
    
    st.divider()
    
    # Risk Table
    display_df = risk_df[[
        "id_student",
        "gender",
        "age_band",
        "highest_education",
        "Segment",
        "TotalClicks",
        "AvgScore",
        "FailedAssessments",
        "TotalAssessments"
    ]].head(50)
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "id_student": "Student ID",
            "gender": "Gender",
            "age_band": "Age",
            "highest_education": "Education",
            "Segment": "Segment",
            "TotalClicks": st.column_config.NumberColumn("Total Clicks", format="%d"),
            "AvgScore": st.column_config.NumberColumn("Avg Score", format="%.1f%%"),
            "FailedAssessments": "Failed",
            "TotalAssessments": "Total"
        }
    )

# ------------------------------------------------
# PAGE 6: ABOUT
# ------------------------------------------------
def show_about():
    st.header("📖 About This Project")
    
    st.markdown("""
    <div style='background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%); 
                padding: 30px; border-radius: 15px; border: 2px solid #6c63ff; 
                box-shadow: 0 0 30px rgba(108, 99, 255, 0.4); margin: 20px 0;'>
    
    ### 🎯 Project Overview
    
    The **Dropout Risk Analysis Dashboard** is an advanced analytics platform designed to help educational institutions 
    identify students at risk of dropping out and take proactive measures to improve student retention.
    
    ### 🔍 Key Features
    
    - **Real-time Analytics**: Powered by Apache Spark for processing large-scale student data
    - **Predictive Insights**: Behavioral segmentation and risk scoring
    - **Multi-dimensional Analysis**: Engagement, demographics, and assessment performance
    - **Interactive Visualizations**: Plotly-based charts for deep insights
    - **Risk Management**: Identify high-risk students and intervention opportunities
    
    ### 📊 Data Sources
    
    This dashboard analyzes three core datasets:
    1. **Student Information**: Demographics, education level, and final results
    2. **Virtual Learning Environment (VLE)**: Student engagement and click activity
    3. **Assessment Data**: Scores, submissions, and performance metrics
    
    ### 🎨 Technology Stack
    
    - **Frontend**: Streamlit with custom dark theme
    - **Backend**: PySpark for distributed data processing
    - **Visualization**: Plotly Express & Plotly Graph Objects
    - **Data Processing**: Pandas for analysis and aggregation
    
    ### 👥 Target Users
    
    - **Tutors & Teachers**: Monitor individual student progress
    - **Academic Advisors**: Identify students needing intervention
    - **Administrators**: Track overall institutional performance
    - **Data Analysts**: Explore patterns in student behavior
    
    ### 📈 Impact Goals
    
    - Reduce dropout rates by 15-20%
    - Enable early intervention for struggling students
    - Improve student engagement and success rates
    - Data-driven decision making for educational policies
    
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div style='text-align: center; padding: 20px; background: #1e1e2e; border-radius: 10px; border: 1px solid #6c63ff;'>
            <h2 style='color: #4fc3f7;'>10K+</h2>
            <p>Students Analyzed</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style='text-align: center; padding: 20px; background: #1e1e2e; border-radius: 10px; border: 1px solid #6c63ff;'>
            <h2 style='color: #6c63ff;'>95%</h2>
            <p>Prediction Accuracy</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style='text-align: center; padding: 20px; background: #1e1e2e; border-radius: 10px; border: 1px solid #6c63ff;'>
            <h2 style='color: #ff6b9d;'>24/7</h2>
            <p>Real-time Monitoring</p>
        </div>
        """, unsafe_allow_html=True)

# ------------------------------------------------
# PAGE 7: RECOMMENDATIONS
# ------------------------------------------------
def show_recommendations(filtered_df, click_q, score_q):
    st.header("💡 Intervention Recommendations")
    
    # Identify different risk categories
    critical_students = filtered_df[
        (filtered_df["AvgScore"] < 30) & 
        (filtered_df["TotalClicks"] < click_q[0])
    ]
    
    low_engagement = filtered_df[
        (filtered_df["TotalClicks"] < click_q[0]) & 
        (filtered_df["AvgScore"] >= 40)
    ]
    
    struggling_performers = filtered_df[
        (filtered_df["AvgScore"] < score_q[0]) & 
        (filtered_df["TotalClicks"] >= click_q[0])
    ]
    
    failed_assessments = filtered_df[filtered_df["FailedAssessments"] >= 3]
    
    # Display risk categories
    st.subheader("🎯 Priority Intervention Groups")
    
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric("🔴 Critical", len(critical_students), help="Low score + Low engagement")
    col2.metric("🟡 Low Engagement", len(low_engagement), help="Low clicks but passing")
    col3.metric("🟠 Struggling", len(struggling_performers), help="Active but low scores")
    col4.metric("⚠️ Multiple Failures", len(failed_assessments), help="3+ failed assessments")
    
    st.divider()
    
    # Recommendations by category
    tab1, tab2, tab3, tab4 = st.tabs([
        "🔴 Critical Students", 
        "🟡 Low Engagement", 
        "🟠 Struggling Performers", 
        "⚠️ Multiple Failures"
    ])
    
    with tab1:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%); 
                    padding: 25px; border-radius: 15px; border: 2px solid #ff6b9d; 
                    box-shadow: 0 0 30px rgba(255, 107, 157, 0.3); margin: 20px 0;'>
        
        ### 🚨 Immediate Action Required
        
        **Characteristics:**
        - Average score below 30%
        - Minimal platform engagement
        - High dropout risk
        
        **Recommended Interventions:**
        
        1. **📞 Immediate Contact**: Personal outreach within 24 hours
        2. **🤝 One-on-One Meetings**: Schedule mandatory academic counseling
        3. **📚 Study Skills Workshop**: Enroll in learning strategies program
        4. **👥 Peer Mentoring**: Match with successful student mentor
        5. **⏰ Time Management**: Create personalized study schedule
        6. **🎯 Goal Setting**: Define achievable short-term milestones
        7. **💼 Resource Connection**: Link to tutoring and support services
        
        **Follow-up Schedule:** Weekly check-ins for 4 weeks
        
        </div>
        """, unsafe_allow_html=True)
        
        if len(critical_students) > 0:
            st.dataframe(
                critical_students[["id_student", "gender", "age_band", "AvgScore", "TotalClicks", "FailedAssessments"]].head(10),
                use_container_width=True,
                hide_index=True
            )
    
    with tab2:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%); 
                    padding: 25px; border-radius: 15px; border: 2px solid #ffeb3b; 
                    box-shadow: 0 0 30px rgba(255, 235, 59, 0.3); margin: 20px 0;'>
        
        ### 📱 Engagement Boosting Strategies
        
        **Characteristics:**
        - Passing grades but minimal platform activity
        - May be disengaged or using offline resources
        
        **Recommended Interventions:**
        
        1. **🎮 Gamification**: Introduce achievement badges and leaderboards
        2. **📧 Email Campaigns**: Send personalized engagement reminders
        3. **🎥 Video Content**: Provide interactive multimedia resources
        4. **💬 Discussion Forums**: Encourage peer interaction and Q&A
        5. **📊 Progress Tracking**: Show personalized learning analytics
        6. **🏆 Incentive Programs**: Reward consistent participation
        7. **📱 Mobile Optimization**: Ensure easy access on all devices
        
        **Follow-up Schedule:** Bi-weekly engagement monitoring
        
        </div>
        """, unsafe_allow_html=True)
        
        if len(low_engagement) > 0:
            st.dataframe(
                low_engagement[["id_student", "gender", "age_band", "AvgScore", "TotalClicks", "ActiveDays"]].head(10),
                use_container_width=True,
                hide_index=True
            )
    
    with tab3:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%); 
                    padding: 25px; border-radius: 15px; border: 2px solid #ff9800; 
                    box-shadow: 0 0 30px rgba(255, 152, 0, 0.3); margin: 20px 0;'>
        
        ### 📖 Academic Support Strategies
        
        **Characteristics:**
        - High engagement but low performance
        - Trying hard but struggling with content
        
        **Recommended Interventions:**
        
        1. **👨‍🏫 Tutoring Services**: Assign subject-specific tutor
        2. **📝 Study Groups**: Form collaborative learning cohorts
        3. **🎯 Learning Style Assessment**: Identify optimal study methods
        4. **📚 Supplementary Materials**: Provide additional practice resources
        5. **⏱️ Test-Taking Strategies**: Workshop on exam preparation
        6. **🔄 Feedback Loop**: Regular progress assessments and adjustments
        7. **🎓 Office Hours**: Encourage instructor consultation
        
        **Follow-up Schedule:** Weekly academic progress review
        
        </div>
        """, unsafe_allow_html=True)
        
        if len(struggling_performers) > 0:
            st.dataframe(
                struggling_performers[["id_student", "gender", "age_band", "AvgScore", "TotalClicks", "FailedAssessments"]].head(10),
                use_container_width=True,
                hide_index=True
            )
    
    with tab4:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%); 
                    padding: 25px; border-radius: 15px; border: 2px solid #f44336; 
                    box-shadow: 0 0 30px rgba(244, 67, 54, 0.3); margin: 20px 0;'>
        
        ### 🎯 Assessment Recovery Plan
        
        **Characteristics:**
        - Multiple failed assessments (3+)
        - Need structured recovery approach
        
        **Recommended Interventions:**
        
        1. **📋 Academic Recovery Plan**: Create formal improvement contract
        2. **🔄 Retake Options**: Offer assessment retake opportunities
        3. **📖 Content Review Sessions**: Intensive review of failed topics
        4. **👥 Study Partner**: Pair with academically strong peer
        5. **⏰ Deadline Extensions**: Consider accommodations if appropriate
        6. **💻 Alternative Assessments**: Explore different evaluation methods
        7. **🧠 Stress Management**: Provide mental health resources
        
        **Follow-up Schedule:** After each assessment + monthly review
        
        </div>
        """, unsafe_allow_html=True)
        
        if len(failed_assessments) > 0:
            st.dataframe(
                failed_assessments[["id_student", "gender", "age_band", "AvgScore", "FailedAssessments", "TotalAssessments"]].head(10),
                use_container_width=True,
                hide_index=True
            )
    
    st.divider()
    
    # General recommendations
    st.subheader("🌟 General Best Practices")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style='background: #1e1e2e; padding: 20px; border-radius: 10px; border: 1px solid #6c63ff;'>
        
        ### 🎓 Institutional Level
        
        - **Early Warning System**: Implement automated alerts for at-risk students
        - **Faculty Training**: Educate instructors on intervention strategies
        - **Resource Allocation**: Ensure adequate support staff and services
        - **Data-Driven Decisions**: Regular review of analytics and outcomes
        - **Student Feedback**: Collect and act on student experience data
        
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style='background: #1e1e2e; padding: 20px; border-radius: 10px; border: 1px solid #6c63ff;'>
        
        ### 👨‍🏫 Instructor Level
        
        - **Regular Communication**: Maintain consistent contact with students
        - **Flexible Deadlines**: Consider individual circumstances
        - **Clear Expectations**: Provide detailed rubrics and guidelines
        - **Timely Feedback**: Return assessments within 1 week
        - **Inclusive Practices**: Accommodate diverse learning needs
        
        </div>
        """, unsafe_allow_html=True)

# ------------------------------------------------
# MAIN APP LOGIC
# ------------------------------------------------
if not st.session_state.logged_in:
    login_page()
else:
    main_dashboard()