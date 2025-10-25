import streamlit as st
import pandas as pd
import pymysql
# connect my database
def create_connection():
  try:
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="Rudra",
            database="policecheck",
            cursorclass=pymysql.cursors.DictCursor         
        )
        return connection
  except Exception as e:
        st.error(f"Database connection Error: {e}")
        return None
  # fetch data 
def fetch_data(query):
     connection = create_connection()
     if connection:
          try:
              with connection.cursor() as cursor:
                   cursor.execute(query)
                   result = cursor.fetchall()
                   secure_check = pd.DataFrame(result)
                   return secure_check
          finally:
               connection.close()
     else:
          return pd.DataFrame()
#streamlit
st.set_page_config(page_title='streamlit project',layout="wide")
st.title("👮‍♀️SecureCheck:Police Post Logs")
st.markdown("⛐ _Real-time logging of vehicles and personnel_")

st.header(" 👉🏻🚥🙎🏻‍♀️violation_stop data" )
#show  data table
query="select*from police_postlog"
securecheck=fetch_data(query)
st.dataframe(securecheck)

#metrics
st.header("🌐🚗key metrics")
colm1,colm2,colm3=st.columns(3)
with colm1:
     stop_vehicle=securecheck.shape[0]
     st.metric("total police stop",stop_vehicle)

with colm2:
     warning=securecheck[securecheck['stop_outcome'].str.contains("warning",case=False)].shape[0]
     st.metric('Total warning',warning)

with colm3:
     total_arrests = securecheck[securecheck['is_arrested']==True].shape[0]
     st.metric('Total_arrests',total_arrests)

#medidum level questions
st.header("🛎Medium insights")

selected_query=st.selectbox("select a query to run",[
     "top 10 drug related stops",
     "most frequently searched vehicles",
     "Highest arrest rate by age group",
     "Stop by gender distrb in each country",
     "Highest search rate by gender&race",
     "Most traffic stop by time",
     "Average stop duration for violation",
     "During night stop leads to arrest",
     "Violation by most search vs. arrest",
     "common younger driver violation<25",
     "kind of violation not search or arrest",
     "Highest drug related rate by country",
     "arrest rate by country and violation",
     "Most stoped country by search conducted"])

query_map={"top 10 drug related stops":"""select vehicle_number,count(*) as drug_stop_count from police_postlog where drugs_related_stop = 1 group BY vehicle_number order BY drug_stop_count desc limit 10""",
           "most frequently searched vehicles":""" select vehicle_number, count(*) as search_count FROM police_postlog where search_conducted = 1 group by vehicle_number order by search_count DESC limit 5""",
           "Highest arrest rate by age group":"""SELECT CASE WHEN driver_age < 18 THEN 'Under 18' WHEN driver_age BETWEEN 18 AND 25 THEN '18-25' WHEN driver_age BETWEEN 26 AND 35 THEN '26-35' WHEN driver_age BETWEEN 36 AND 45 THEN '36-45' WHEN driver_age BETWEEN 46 AND 60 THEN '46-60' ELSE '60+'END AS age_group, COUNT(*) AS total_stops, SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0  END) AS total_arrests , ROUND(SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS arrest_rate_percent FROM police_postlog WHERE driver_age GROUP BY age_group ORDER BY arrest_rate_percent DESC LIMIT 1""",
           "Stop by gender distrb in each country":"""SELECT country_name,driver_gender,COUNT(*) AS total_stops,ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY country_name),2) AS percentage_of_stops FROM police_postlog WHERE driver_gender is not null AND country_name IS NOT NULL GROUP BY country_name, driver_gender ORDER BY country_name, percentage_of_stops DESC""",
           "Highest search rate by gender&race":"""SELECT driver_race,driver_gender,COUNT(*) AS total_stops,SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) AS total_searches, (SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS search_rate from police_postlog  GROUP BY driver_race, driver_gender order by search_rate DESC LIMIT 3""",
           "Most traffic stop by time":"""SELECT CONCAT(LPAD(FLOOR(MINUTE(stop_time)/15)*15, 2, '0'), '-', LPAD(FLOOR(MINUTE(stop_time)/15)*15 + 14, 2, '0')) AS time_interval,HOUR(stop_time) AS stop_hour,COUNT(*) AS total_stops FROM police_postlog GROUP BY stop_hour, time_interval ORDER BY total_stops DESC""",
           "Average stop duration for violation":"""SELECT violation, AVG(stop_duration) AS avg_duration_minutes FROM police_postlog GROUP BY violation ORDER BY avg_duration_minutes DESC""",
           "During night stop leads to arrest":"""SELECT CASE WHEN HOUR(stop_time) BETWEEN 18 AND 23 THEN 'Evening (6 PM - 11 PM)' WHEN HOUR(stop_time) BETWEEN 0 AND 5 THEN 'Night (12 AM - 5 AM)' WHEN HOUR(stop_time) BETWEEN 6 AND 11 THEN 'Morning (6 AM - 11 AM)'ELSE 'Afternoon (12 PM - 5 PM)' END AS time_of_day,SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS total_arrests, ROUND(SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS arrest_rate_percent FROM police_postlog GROUP BY time_of_day ORDER BY arrest_rate_percent DESC""",
           "Violation by most search vs. arrest":"""SELECT violation, COUNT(*) AS total_stops,SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) AS total_searches, SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS total_arrests, ROUND(SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS search_rate_percent,ROUND(SUM(CASE WHEN is_arrested= 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS arrest_rate_percent FROM police_postlog GROUP BY violation ORDER BY (search_rate_percent + arrest_rate_percent) DESC LIMIT 10""",
          "common younger driver violation<25":"""SELECT violation,COUNT(*) AS total_violations FROM police_postlog WHERE driver_age < 25 GROUP BY violation ORDER BY total_violations DESC LIMIT 10""",
          "kind of violation not search or arrest":"""SELECT violation,COUNT(*) AS total_stops,SUM(CASE WHEN search_conducted = 0 THEN 1 ELSE 0 END) AS not_search,SUM(CASE WHEN is_arrested = 0 THEN 1 ELSE 0 END) AS not_arrests,ROUND(SUM(CASE WHEN search_conducted = 0 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS search_rate_percent, ROUND(SUM(CASE WHEN is_arrested = 0 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS arrest_rate_percent FROM police_postlog GROUP BY violation HAVING total_stops > 10  ORDER BY (search_rate_percent + arrest_rate_percent) ASC """,
          "Highest drug related rate by country":"""SELECT country_name,COUNT(*) AS total_stops,SUM(CASE WHEN drugs_related_stop = 1 THEN 1 ELSE 0 END) AS drug_related_stops, ROUND(SUM(CASE WHEN drugs_related_stop = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS drug_related_rate_percent FROM police_postlog WHERE country_name is not null GROUP BY country_name ORDER BY drug_related_rate_percent DESC""",
          "arrest rate by country and violation":"""SELECT country_name,violation,COUNT(*) AS total_stops,SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS total_arrests,ROUND(SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),2) AS arrest_rate_percent FROM police_postlog WHERE country_name is not null AND violation is not null GROUP BY country_name, violation ORDER BY country_name,arrest_rate_percent DESC""",
          "Most stoped country by search conducted":"""SELECT country_name,COUNT(*) AS total_search_stops FROM police_postlog WHERE search_conducted = 1 GROUP BY country_name ORDER BY total_search_stops DESc LIMIT 1"""}

if st.button("Run query"):
     result=fetch_data(query_map[selected_query])
     if not result.empty:
          st.write(result)
     else:
          st.warning("No result found for the selected query")


#complex level queries
st.header("👉Advanced insights")
selected_query1=st.selectbox("select a query to run",["yearwise arrest by country",
                                                      "violation based on age and race",
                                                      "Time period analysis",
                                                      "HIghsearch and arrest by violation",
                                                      "Driver demographic by country",
                                                      "Top 5 violation with high arrest rate"])

query_map1={"yearwise arrest by country":"""SELECT country_name,stop_year,total_stops,total_arrests,ROUND(total_arrests * 100.0 / total_stops, 2) AS arrest_rate_percent,ROUND(total_stops * 100.0 / SUM(total_stops) OVER (PARTITION BY country_name), 2) AS pct_of_country_stops
                                            FROM (SELECT country_name,YEAR(stop_date) AS stop_year,COUNT(*) AS total_stops,SUM(CASE WHEN is_arrested= 1 THEN 1 ELSE 0 END) AS total_arrests FROM police_postlog WHERE country_name IS NOT NULL AND stop_time IS NOT NULL GROUP BY country_name,YEAR(stop_date)) AS yearly_stats ORDER BY country_name,stop_year""",
            "violation based on age and race":"""SELECT age_race.age_group,age_race.driver_race,v.violation,v.total_stops,ROUND(v.total_stops * 100.0 / age_race.group_total_stops, 2) AS pct_of_group
                                             FROM (SELECT CASE WHEN driver_age < 18 THEN 'Under 18' WHEN driver_age BETWEEN 18 AND 25 THEN '18-25' WHEN driver_age BETWEEN 26 AND 35 THEN '26-35' WHEN driver_age BETWEEN 36 AND 45 THEN '36-45' WHEN driver_age BETWEEN 46 AND 60 THEN '46-60' ELSE '60+' END AS age_group,driver_race,COUNT(*) AS group_total_stops FROM police_postlog WHERE driver_age IS NOT NULL
                                             AND driver_race IS NOT NULL GROUP BY age_group, driver_race) AS age_race JOIN(SELECT CASE WHEN driver_age < 18 THEN 'Under 18'WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'WHEN driver_age BETWEEN 26 AND 35 THEN '26-35'WHEN driver_age BETWEEN 36 AND 45 THEN '36-45'WHEN driver_age BETWEEN 46 AND 60 THEN '46-60'ELSE '60+'END AS age_group,driver_race,
                                            violation,COUNT(*) AS total_stops FROM police_postlog  WHERE driver_age IS NOT NULL AND driver_race IS NOT NULL AND violation IS NOT NULL GROUP BY age_group, driver_race, violation) AS v ON age_race.age_group = v.age_group AND age_race.driver_race = v.driver_race ORDER BY age_race.age_group, age_race.driver_race, v.total_stops DESC""",
             "Time period analysis":        """SELECT YEAR(STR_TO_DATE(CONCAT(stop_date, ' ', stop_time), '%Y-%m-%d %H:%i:%s')) AS stop_year,MONTH(STR_TO_DATE(CONCAT(stop_date, ' ', stop_time), '%Y-%m-%d %H:%i:%s')) AS stop_month,HOUR(STR_TO_DATE(CONCAT(stop_date, ' ', stop_time), '%Y-%m-%d %H:%i:%s')) AS stop_hour,COUNT(*) AS total_stops
                                              FROM police_postlog WHERE stop_date IS NOT NULL AND stop_time IS NOT NULL GROUP BY stop_year, stop_month, stop_hour ORDER BY stop_year, stop_month, stop_hour""",
              "HIghsearch and arrest by violation":"""SELECT violation,COUNT(*) AS total_stops,SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) AS total_searches,SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS total_arrests,ROUND(SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS search_rate_percent,ROUND(SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percent,
                                                  RANK() OVER (ORDER BY ROUND(SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) DESC) AS search_rate_rank,
                                                  RANK() OVER (ORDER BY ROUND(SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) DESC) AS arrest_rate_rank FROM police_postlog WHERE violation IS NOT NULL GROUP BY violation ORDER BY search_rate_percent DESC, arrest_rate_percent DESC LIMIT 10""",                         
           "Driver demographic by country":"""SELECT country_name,ROUND(AVG(driver_age), 2) AS avg_driver_age,COUNT(*) AS total_drivers,SUM(CASE WHEN driver_gender = 'M' THEN 1 ELSE 0 END) AS male_drivers,SUM(CASE WHEN driver_gender = 'F' THEN 1 ELSE 0 END) AS female_drivers,ROUND(SUM(CASE WHEN driver_gender = 'M' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS male_percent,ROUND(SUM(CASE WHEN driver_gender = 'F' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS female_percent,
                                               SUM(CASE WHEN driver_race IS NOT NULL THEN 1 ELSE 0 END) AS known_race_count,COUNT(DISTINCT driver_race) AS total_races FROM police_postlog WHERE country_name IS NOT NULL AND driver_age IS NOT NULL AND driver_gender IS NOT NULL GROUP BY country_name ORDER BY total_drivers DESC""",
           "Top 5 violation with high arrest rate":"""SELECT violation,COUNT(*) AS total_stops,SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS total_arrests,ROUND(SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percent FROM police_postlog WHERE violation IS NOT NULL GROUP BY violation ORDER BY arrest_rate_percent DESC LIMIT 5"""}



if st.button("run_ query_button"):
     result=fetch_data(query_map1[selected_query1])
     if not result.empty:
          st.write(result)
     else:
          st.warning("No result found for the selected query")

st.markdown("˙✧˖°⚖︎⋆｡ ˚Law Enforcement & Public Safety")
st.header("**🔍Filter the violation by age,race**")

st.header("💻securecheck for most frequent violation & predict outcome and violation")

#input for all field
with st.form("new_police_form"):
     stop_date=st.date_input("stop Date")
     stop_time=st.time_input("stop Time")
     country_name=st.selectbox("country Name",["India","USA","Canada"])
     driver_gender=st.selectbox("Driver Gender",["Male","Female"])
     driver_age=st.number_input("Driver Age",min_value=16,max_value=100,value=33)
     driver_race=st.text_input("Driver Race")
     search_conducted=st.selectbox("was a search conducted?",["0","1"])
     search_type=st.text_input("search Type")
     drugs_related_stop=st.selectbox("was it drug Related?",["0","1"])
     stop_duration=st.selectbox("stop Duration",securecheck['stop_duration'].dropna().unique())
     vehicle_number=st.text_input("vehicle Number")
     timestamp=pd.Timestamp.now()

     submitted=st.form_submit_button("predict  violation")
     filtered_data = pd.DataFrame()

if submitted:
     filtered_data=securecheck[(securecheck['driver_gender']==driver_gender)&
                        (securecheck['driver_age']==driver_age)&
                        (securecheck['search_conducted']==int(search_conducted))&
                        (securecheck['stop_duration']==stop_duration)&
                        (securecheck['drugs_related_stop']==int(drugs_related_stop))&
                        (securecheck['vehicle_number']==vehicle_number)]

 #Predict stop_outcome
if not filtered_data .empty:
     predicted_outcome=filtered_data['stop_outcome'].mode()[0]
     predicted_violation=filtered_data['violation'].mode()[0]  
else:
     predicted_outcome="warning"
     predicted_violation="speeding"

#summary
search_statement="A search was conducted"if int(search_conducted)else "No search was conducted"            
drug_statement=" was drug_related"if int(drugs_related_stop)else "was not druged"

st.markdown(f"""
            **prediction summary**
            
     **predicted violation**:{predicted_violation}

     **predicted stop outcome**:{predicted_outcome}
            
          🎰A {driver_age}-year-old    {driver_gender}   driver  in {country_name}  
            was stopped at {stop_time.strftime('%I:%M %p')}"
            on {stop_date}.and  They {search_statement}, and the stop {drug_statement}.
              stop duration:**{stop_duration}**
              vehicle Number:**{vehicle_number}**.""")