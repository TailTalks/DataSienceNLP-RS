using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.IO;
using System.Data.SqlClient;
using Newtonsoft.Json;
using System.Configuration;

namespace IntegrationWithOrion
{
    class Program
    {
        static string path = @"logIntegrationWithOrion.txt";
        static FileStream fs = new FileStream(path, FileMode.Append, FileAccess.Write);
        static StreamWriter sw = new StreamWriter(fs);
        static int dataStart = Int32.Parse(ConfigurationManager.AppSettings["dataStart"]);
        static int dataFinish = Int32.Parse(ConfigurationManager.AppSettings["dataFinish"]);
        static string login = ConfigurationManager.AppSettings["login"];
        static string password = ConfigurationManager.AppSettings["password"].ToString();
        static SqlConnection GetConnection(int app=0)
        {
            SqlConnection conn = new SqlConnection();

            conn.ConnectionString = @"Data Source=srvsp06;Initial Catalog=Domiland;User ID="+login+"; Password="+password+"";
            if (app == 2) conn.ConnectionString = @"Data Source=srvsp06;Initial Catalog=DomilandStat;User ID=mobile; Password=tran29klZ";
            if (app == 1) conn.ConnectionString = @"Data Source=srvsp06;Initial Catalog=Mobile;User ID=mobile; Password=";
            try
            {
                conn.Open();
            }
            catch (Exception ex)
            {
                Console.Write(ex.Message);
                conn = null;
            }
            return conn;

        }
        static List<string> getConnpar(string app)
        {
            List<string> res = new List<string>();
            SqlConnection conn = new SqlConnection();
            try
            {

                conn.ConnectionString = @"Data Source=MSSQLCLS;Initial Catalog=ConnectData;Trusted_Connection=Yes";


                conn.Open();

            }

            catch (Exception ex)
            {

                Console.WriteLine("Can't open connection " + conn.ConnectionString.Substring(0, 25) + ": " + ex.Message);
                sw.WriteLine(DateTime.Now.ToString() + "    GetConnPar " + ex.Message);

            }
            if (conn != null)
            {
                string text = "Select servname,basename from Conn where appname='" + app + "'";
                SqlCommand comm = new SqlCommand();
                comm.Connection = conn;
                comm.CommandText = text;
                SqlDataReader dr = null;
                try
                {
                    dr = comm.ExecuteReader();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Can't read " + ": " + ex.Message);
                    sw.WriteLine(DateTime.Now.ToString() + "    ERR read" + ex.Message);
                    if (dr != null) dr.Close();
                }

                if (dr != null)
                {
                    while (dr.Read())
                    {
                        res.Add(dr[0].ToString());
                        res.Add(dr[1].ToString());
                    }
                    dr.Close();
                }
            }
            conn.Close();
            return res;
        }
        static void Main(string[] args)
        {
            string token = "";

            token = GetToken2();
            Rootobject rt = JsonConvert.DeserializeObject<Rootobject>(token);
            token = rt.token;
            GetBuildings(token, 2);
            GetOrdersUpDatesAt(token, 2);
            GetCustomers(token, 2);
            GetPlaces(token, 2);
            GetServices(token, 2);
            sw.Flush();
        }
        
        static Boolean isUserInList(string id, string list)
        {
            Boolean re = false;
            if (list.Contains(id)) re = true;
            return re;
        }
       
        static Boolean IsExistObject(string fieldnameID, string valueID, string tablename, SqlConnection conn, string orderid="")
        {
            Boolean res = false;
            SqlCommand comm = new SqlCommand();
            comm.Connection = conn;
            comm.CommandText = " select "+fieldnameID+" from "+tablename+" where "+ fieldnameID+" = " + valueID.Trim();
            if (orderid != "") comm.CommandText += " and orderId=" + orderid;
            SqlDataReader dr = null;
            try
            {
                dr = comm.ExecuteReader();
                if (dr.HasRows) res = true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Can't read "+tablename+" : " + ex.Message);
                sw.WriteLine(DateTime.Now.ToString() + "    ERR read " + tablename  + ex.Message);

            }
            if (dr != null) dr.Close();
            return res;
        }
        public static DateTime UnixTimeStampToDateTime(double unixTimeStamp)
        {
            // Unix timestamp is seconds past epoch
            System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddSeconds(unixTimeStamp).ToLocalTime();
            return dtDateTime;
        }

        static void GetDataOrder(string token,int id ,int app = 0)
        {
            string orders = GetObjects("https://sud-api.domyland.ru/orders/"+id, token);
            Console.WriteLine(orders);
            Console.ReadLine();
        }

        static void GetOrders(string token, int app=0)
        {
            int rown = 0;
            string listuser = "";
            string orders = GetObjects("https://sud-api.domyland.ru/orders", token);
            orders = orders.Replace("null", "0");
            orders = orders.Replace("\"placeEntranceNumber\":\"\"", "\"placeEntranceNumber\":\"0\"");
            Orders ord = JsonConvert.DeserializeObject<Orders>(orders);
            string text = "";
            using (SqlConnection conn = GetConnection(app))
            {
                if (conn != null)
                   
                while (ord!=null && ord.items.Length > 0)
                    {
                        text = "";
                        if (conn == null) GetConnection(app);
                        if (conn != null)
                        {
                            text = "";
                            foreach (Order order in ord.items)
                            {
                                Responsibleuser[] user = order.responsibleUsers;
                                string orderid = order.id.ToString();
                                Boolean isEx = IsExistObject("orderId",orderid,"Orders", conn);
                                int isAccident = 0;
                                int isPaidByLK = 0;
                                int hasOrderRefundWarning = 0;
                                int isManagementContractSigned = 0;
                                int isAcceptanceCertificateSigned = 0;
                                if (order.isAccident.ToString() == "true") isAccident = 1;
                                if (order.isPaidByLK.ToString() == "true") isPaidByLK = 1;
                                if (order.hasOrderRefundWarning.ToString() == "true") hasOrderRefundWarning = 1;
                                if (order.isManagementContractSigned.ToString() == "true") isManagementContractSigned = 1;
                                if (order.isAcceptanceCertificateSigned.ToString() == "true") isAcceptanceCertificateSigned = 1;


                                if (isEx)
                                {
                                    text += " update Orders set  orderStatusComment='" + order.orderStatusComment + "',updatedAt=" + order.updatedAt.ToString().Replace(",", ".") + ",creditSum=" + order.creditSum.ToString().Replace(",", ".") + ",paidSum=" +
                                        order.paidSum.ToString().Replace(",", ".") + ",refundSum=" + order.refundSum.ToString().Replace(",", ".") + ",totalSum=" + order.totalSum.ToString().Replace(",", ".") + ",invoiceStatusId=" + order.invoiceStatusId.ToString().Replace(",", ".") + ",isPaidByLK=" +
                                        isPaidByLK.ToString().Replace(",", ".") + ",orderRefundSum=" + order.orderRefundSum.ToString().Replace(",", ".") + ",orderRefundStatusId=" + order.orderRefundStatusId.ToString().Replace(",", ".") + ",hasOrderRefundWarning=" + hasOrderRefundWarning +
                                        ",customerMainDebt=" + order.customerMainDebt.ToString().Replace(",", ".") + ",customerCapitalRepairDebt=" + order.customerCapitalRepairDebt.ToString().Replace(",", ".") + ",orderStatusId=" + order.orderStatusId.ToString().Replace(",", ".") +
                                        ",orderStatusTitle='" + order.orderStatusTitle + "',orderStatusColor='" + order.orderStatusColor + "',orderStatusColorName='" + order.orderStatusColorName + "',customerDebt=" + order.customerDebt.ToString().Replace(",", ".") +
                                        ",isManagementContractSigned=" + isManagementContractSigned.ToString().Replace(",", ".") + ",closeOrderUser='" + order.closeOrderUser + "',closeOrderAt=" + order.closeOrderAt.ToString().Replace(",", ".") + ",closeOrderUserFullName='" +
                                        order.closeOrderUserFullName + "',historyStatus='" + order.historyStatus + "' , performersAllStatuses='"+order.performersAllStatuses.Replace("'", "`").Replace("\"","`") + "',serviceTypeTitle='" + order.serviceTypeTitle + "'"+
                                        ",serviceTitle='" + order.serviceTitle + "',placeEntranceNumber='"+order.placeEntranceNumber.ToString().Replace(",", ".") + "'"+
                                        ", buildingTitle='" + order.buildingTitle + "',customerSummary='" +order.customerSummary + "',placeAddress='" + order.placeAddress + "'"+
                                        ",rating=" + order.rating.ToString().Replace(",", ".") + ", sourceTitle='"+ order.sourceTitle + "', customerId=" + order.customerId.ToString().Replace(",", ".") + "+" +
                                        ",placeTypeShortTitle='" + order.placeTypeShortTitle + "',placeNumber='" + order.placeNumber + "',customerShortName='" + order.customerShortName + "',workOrderUserFullName='" + order.workOrderUserFullName +
                                        "',buildingExtId='" + order.buildingExtId + "' where orderId=" + orderid;
                                }
                                else
                                {
                                    text += " insert into Orders (orderId,rating,ratingComment,isAccident,issueId,sourceTitle,orderSourceTitle,orderStatusComment," +
                                        "solveTimeSLA,solveTimeFact," +
                                        "reactionTimeSLA,reactionTimeFact,orderTypeId,orderTypeTitle,orderIssueTypeId,orderIssueTypeTitle,serviceTitle,userWarning,serviceTypeId," +
                                        "serviceTypeTitle,serviceTypeImage,createdAt,updatedAt,createdByUser,creditSum,paidSum,refundSum,totalSum,invoiceStatusId,isPaidByLK,unregisteredCustomerName," +
                                        "unregisteredPhoneNumber,unregisteredEmail,unregisteredAddress,orderRefundSum,orderRefundStatusId,hasOrderRefundWarning,buildingId," +
                                        "buildingExtId,buildingTitle,placeId,placeExtId,placeTitle,placeNumber,placeAddress,placeFloor,placeEntranceNumber,accountNumber,placeTypeTitle," +
                                        "placeTypeShortTitle,customerId,customerFullName,customerShortName,customerPhoneNumber,customerImage,customerTypeId,customerSummary,customerStatus," +
                                        "customerMainDebt,customerCapitalRepairDebt,orderStatusId,orderStatusTitle,orderStatusColor,orderStatusColorName,customerDebt,isManagementContractSigned," +
                                        "isAcceptanceCertificateSigned,acceptedOrderUser,acceptedOrderAt,acceptedOrderUserFullName,workOrderAt,workOrderUserFullName,closeOrderUser,closeOrderAt," +
                                        "closeOrderUserFullName,historyStatus,performersAllStatuses,managerSubGroup,unviewedCommentsCount)" +
                                        " values(" + order.id.ToString().Replace(",", ".") + "," + order.rating.ToString().Replace(",", ".") + ",'" + order.ratingComment + "'," + isAccident.ToString().Replace(",", ".") + "," + order.issueId.ToString().Replace(",", ".") + ",'" +
                                        order.sourceTitle + "','" + order.orderSourceTitle + "','" + order.orderStatusComment + "'," +
                                        order.solveTimeSLA.ToString().Replace(",", ".") + "," + order.solveTimeFact.ToString().Replace(",", ".") + "," +
                                        order.reactionTimeSLA.ToString().Replace(",", ".") + "," + order.reactionTimeFact.ToString().Replace(",", ".") + "," + order.orderTypeId.ToString().Replace(",", ".") + ",'" + order.orderTypeTitle + "'," +
                                        order.orderIssueTypeId.ToString().Replace(",", ".") + ",'" + order.orderIssueTypeTitle + "','" + order.serviceTitle + "'," + order.userWarning.ToString().Replace(",", ".") + "," +
                                        order.serviceTypeId.ToString().Replace(",", ".") + ",'" + order.serviceTypeTitle + "','" + order.serviceTypeImage + "'," + order.createdAt.ToString().Replace(",", ".") + "," +
                                        order.updatedAt.ToString().Replace(",", ".") + ",'" + order.createdByUser + "'," + order.creditSum.ToString().Replace(",", ".") + "," + order.paidSum.ToString().Replace(",", ".") + "," + order.refundSum.ToString().Replace(",", ".") + "," +
                                        order.totalSum.ToString().Replace(",", ".") + "," + order.invoiceStatusId.ToString().Replace(",", ".") + "," + isPaidByLK.ToString().Replace(",", ".") + ",'" + order.unregisteredCustomerName + "','" +
                                        order.unregisteredPhoneNumber + "','" + order.unregisteredEmail + "','" + order.unregisteredAddress + "'," + order.orderRefundSum.ToString().Replace(",", ".") + "," +
                                        order.orderRefundStatusId.ToString().Replace(",", ".") + "," + hasOrderRefundWarning + "," + order.buildingId.ToString().Replace(",", ".") + ",'" + order.buildingExtId + "','" + order.buildingTitle + "'," +
                                        order.placeId.ToString().Replace(",", ".") + ",'" + order.placeExtId + "','" + order.placeTitle + "','" + order.placeNumber + "','" + order.placeAddress + "'," + order.placeFloor.ToString().Replace(",", ".") + ",'" +
                                        order.placeEntranceNumber.ToString().Replace(",", ".") + "','" + order.accountNumber + "','" + order.placeTypeTitle + "','" + order.placeTypeShortTitle + "'," + order.customerId.ToString().Replace(",", ".") + ",'" +
                                        order.customerFullName + "','" + order.customerShortName + "','" + order.customerPhoneNumber + "','" + order.customerImage + "'," + order.customerTypeId.ToString().Replace(",", ".") + ",'" +
                                        order.customerSummary + "','" + order.customerStatus + "'," + order.customerMainDebt.ToString().Replace(",", ".") + "," + order.customerCapitalRepairDebt.ToString().Replace(",", ".") + "," + order.orderStatusId.ToString().Replace(",", ".") + ",'" +
                                       order.orderStatusTitle + "','" + order.orderStatusColor + "','" + order.orderStatusColorName + "'," + order.customerDebt.ToString().Replace(",", ".") + "," + isManagementContractSigned.ToString().Replace(",", ".") + "," +
                                       isAcceptanceCertificateSigned.ToString().Replace(",", ".") + ",'" + order.acceptedOrderUser + "'," + order.acceptedOrderAt.ToString().Replace(",", ".") + ",'" + order.acceptedOrderUserFullName + "'," +
                                       order.workOrderAt.ToString().Replace(",", ".") + ",'" + order.workOrderUserFullName + "','" + order.closeOrderUser + "'," + order.closeOrderAt.ToString().Replace(",", ".") + ",'" + order.closeOrderUserFullName + "','" +
                                       order.historyStatus + "','" + order.performersAllStatuses.Replace("'", "`").Replace("\"", "`") + "','" + order.managerSubGroup + "'," + order.unviewedCommentsCount.ToString().Replace(",", ".") + ") ";
                                    for (int i = 0; i < user.Length; i++)
                                    {
                                        if (!isUserInList(user[i].id.ToString().Trim(), listuser))
                                        {
                                            if (listuser.Length > 0) listuser += ";";
                                            listuser += user[i].id.ToString().Trim();
                                            Boolean isUserE = IsExistObject("userId", user[i].id.ToString().Trim(), "Users", conn);
                                            
                                            if (isUserE == false)
                                            {
                                                text += " insert into Users(userId,firstName,lastName,fullName)values(" + user[i].id.ToString() + ",'" + user[i].firstName + "','" + user[i].lastName + "','" + user[i].fullName + "') ";
                                            }
                                                
                                        }
                                       
                                        
                                        text += " insert into OrderResponsibleuser (userId,orderId) values (" + user[i].id.ToString() + "," + orderid + ") ";
                                       // text += " insert into OrderResponsibleuserTest (userId,orderId) values (" + user[i].id.ToString() + "," + orderid + ") ";
                                    }

                                }

                            }
                            if (text != "")
                            {
                               

                                SqlCommand comm = new SqlCommand();
                                comm.Connection = conn;
                                comm.CommandText = text;
                                try
                                {
                                    int ii = comm.ExecuteNonQuery();
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("Can't write Orders" + ": " + ex.Message);
                                    sw.WriteLine(DateTime.Now.ToString() + "    ERR wrute Orders" + ex.Message);
                                }
                            }
                        }
                        rown = ord.nextRow;
                        Console.WriteLine("next ="+ ord.nextRow.ToString());
                        orders = GetObjects("https://sud-api.domyland.ru/orders?fromRow=" + rown.ToString(), token);
                        orders = orders.Replace("null", "0");
                        orders = orders.Replace("\"placeEntranceNumber\":\"\"", "\"placeEntranceNumber\":\"0\"");
                        ord = JsonConvert.DeserializeObject<Orders>(orders);

                    }
                 text = "update Orders set solveTimeSLAD=case when solveTimeSLA=0 then null else dateadd(hour,3,dateadd(S, [solveTimeSLA], '1970-01-01 00:00:00')) end " +
  " update Orders set solveTimeFactD =case when solveTimeFact = 0 then null else dateadd(hour, 3, dateadd(S, [solveTimeFact], '1970-01-01 00:00:00')) end " +
  " update Orders set reactionTimeSLAD =case when reactionTimeSLA = 0 then null else dateadd(hour, 3, dateadd(S, [reactionTimeSLA], '1970-01-01 00:00:00')) end " +
  "  update Orders set reactionTimeFactD =case when reactionTimeFact = 0 then null else dateadd(hour, 3, dateadd(S, [reactionTimeFact], '1970-01-01 00:00:00')) end " +
   "  update Orders set createdAtD =case when createdAt = 0 then null else dateadd(hour, 3, dateadd(S, [createdAt], '1970-01-01 00:00:00')) end " +
   "  update Orders set updatedAtD =case when updatedAt = 0 then null else dateadd(hour, 3, dateadd(S, [updatedAt], '1970-01-01 00:00:00')) end " +
    " update Orders set acceptedOrderAtD =case when acceptedOrderAt = 0 then null else dateadd(hour, 3, dateadd(S, [acceptedOrderAt], '1970-01-01 00:00:00')) end " +
   "  update Orders set workOrderAtD =case when workOrderAt = 0 then null else dateadd(hour, 3, dateadd(S, [workOrderAt], '1970-01-01 00:00:00')) end " +
    " update Orders set closeOrderAtD =case when closeOrderAt = 0 then null else dateadd(hour, 3, dateadd(S, [closeOrderAt], '1970-01-01 00:00:00')) end ";

                SqlCommand comm1 = new SqlCommand();
                comm1.Connection = conn;
                comm1.CommandText = text;
                try
                {
                    int ii = comm1.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Can't write Orders" + ": " + ex.Message);
                    sw.WriteLine(DateTime.Now.ToString() + "    ERR wrute Orders" + ex.Message);
                }
            }
                
           
           //  Console.ReadKey();
        }

        static void GetPlaces(string token,int app=0)
        {
            int row = 0;
            string list = "";
            string p = GetObjects("https://sud-api.domyland.ru/places?fromRow=" + row.ToString(), token);
            //cust = cust.Replace("'", "\"").Replace("null", "0"); 
            p = p.Replace("null", "0");
            Places places = JsonConvert.DeserializeObject<Places>(p);
            using (SqlConnection conn = GetConnection(app))
            {
                if (conn != null)
                {
                    while (places!=null && places.items.Length > 0)
                    {
                        if (conn == null) GetConnection(app);
                        if (conn != null)
                        {
                            string text = "";
                            foreach(Place pl in places.items)
                            {
                                if (!isUserInList(pl.id.ToString().Trim(), list))
                                {
                                    if (list.Length > 0) list += ";";
                                    list += pl.id.ToString().Trim();
                                    Boolean isCust = IsExistObject("placeId", pl.id.ToString(), "Places", conn);
                                    if (!isCust)
                                    {
                                        text += " insert into Places (placeId," +
                                            "extId," +
                                            "title,number,street,address," +
                                            "floor," +
                                            //"entranceNumber," +
                                            "areaSize,totalAreaSize,houseNumber,buildingId,"+
                                            "placeTypeId,placeTypeTitle,placeTypeShortTitle,placeTypeImage) values" +
                                            "("+pl.id.ToString().Replace(",", ".") + 
                                            ",'"+pl.extId +
                                            "','"+pl.title.Replace("'", "`") + 
                                            "','"+pl.number.Replace("'", "`") + 
                                            "','"+pl.street.Replace("'", "`") + "','"+pl.address.Replace("'", "`") + 
                                            "',"+pl.floor.ToString().Replace(",",".")+",'"+ 
                                           //pl.entranceNumber.ToString().Replace(",", ".") + 
                                           "','"+pl.areaSize +"','"+pl.totalAreaSize +"','"+pl.houseNumber +"',"+pl.buildingId.ToString().Replace(",", ".") + ","+
                                           pl.placeTypeId.ToString().Replace(",",".")+",'"+pl.placeTypeTitle +"','"+pl.placeTypeShortTitle +"','"+pl.placeTypeImage+"' )";
                                       foreach(Account a in pl.accounts)
                                        {
                                            text += " insert into AccountPlace(placeId, accountId , number, numberGIS) values (" + pl.id.ToString().Replace(",", ".") + "," + a.id.ToString().Replace(",", ".") + ",'"+a.number+"','"+
                                                a.numberGIS+"') ";
                                        }
                                      foreach(Placetocustomer ptc in pl.placeToCustomer)
                                        {
                                            text += " insert into  PlaceToCustomer (placeId,customerId," +
                                                "certificate," +
                                                "customerTypeId,paymentAllowed," +
                                                //"certificateDate," +
                                                "customerFullName,"+
                                                "placeToCustomerId," +
                                                "managementContract," +
                                                //"constructionContract," +
                                                "acceptanceCertificate," +
                                                "managementContractDate,placeToCustomerStatusId,constructionContractDate,acceptanceCertificateDate,placeToCustomerStatusTitle) values " +
                                                "(" + pl.id.ToString().Replace(",",".") + ","+ptc.customerId.ToString().Replace(",",".")+
                                                ",'"+ptc.certificate+
                                                "',"+ptc.customerTypeId.ToString().Replace(",",".")+","+
                                                ptc.paymentAllowed.ToString().Replace(",",".")+",'"+
                                                //ptc.certificateDate.ToString().Replace(",",".")+"','"+
                                                ptc.customerFullName+"',"+
                                                ptc.placeToCustomerId.ToString().Replace(",",".")+",'" +
                                                ptc.managementContract+"','"+
                                                //ptc.constructionContract.ToString().Replace(",",".")+"','"+
                                                ptc.acceptanceCertificate+
                                                "',"+ptc.managementContractDate.ToString().Replace(",",".")+"," +
                                               ptc.placeToCustomerStatusId.ToString().Replace(",",".")+",'"+ptc.constructionContractDate +
                                               "','"+ptc.acceptanceCertificateDate +"','"+ptc.placeToCustomerStatusTitle +"') ";
                                        }
                                    }
                                    else
                                    {

                                    }
                                }
                            }
                            if (text != "")
                            {
                                SqlCommand comm = new SqlCommand();
                                comm.CommandText = text;
                                comm.Connection = conn;
                                try
                                {
                                    int jj = comm.ExecuteNonQuery();
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("Can't write Places" + ": " + ex.Message);
                                    sw.WriteLine(DateTime.Now.ToString() + "    ERR write Places" + ex.Message);
                                }
                            }

                        }
                        row = places.nextRow;
                        Console.WriteLine("next =" + row);
                        p = GetObjects("https://sud-api.domyland.ru/places?fromRow=" + row.ToString(), token);
                        //cust = cust.Replace("'", "\"").Replace("null", "0"); 
                        p = p.Replace("null", "0");
                        places = JsonConvert.DeserializeObject<Places>(p);
                    }
                }
            }
        }
        static void GetServices(string token,int app)
        {
            int row = 0;
            string list = "";
            string p = GetObjects("https://sud-api.domyland.ru/services?fromRow=" + row.ToString(), token);
            //cust = cust.Replace("'", "\"").Replace("null", "0"); 
            p = p.Replace("null", "0");
           Services ss = JsonConvert.DeserializeObject<Services>(p);
            using (SqlConnection conn = GetConnection(app))
            {
                if (conn != null)
                {
                    while (ss!=null && ss.items.Length > 0)
                    {
                        if (conn == null) GetConnection(app);
                        if (conn != null)
                        {
                            string text = "";
                            foreach (Service s in ss.items)
                            {
                                if (!isUserInList(s.id.ToString().Trim(), list))
                                {
                                    if (list.Length > 0) list += ";";
                                    list += s.id.ToString().Trim();
                                    Boolean isCust = IsExistObject("serviceId", s.id.ToString(), "ServicesD", conn);
                                    if (!isCust)
                                    {
                                        text += " insert into ServicesD (serviceId,icon,image,title,ordering,serviceStatusId,serviceStatusTitle,serviceStatusColor,"+
                                            "serviceStatusColorName,serviceTypeId,serviceTypeTitle,isPayable,buildingTitles) values" +
                                            "(" + s.id.ToString()+",'"+s.icon +"','"+s.image +"','"+s.title +"',"+s.ordering.ToString() +","+s.serviceStatusId.ToString()+",'"+ 
                                           s.serviceStatusTitle+"','"+s.serviceStatusColor +"','"+s.serviceStatusColorName +"',"+s.serviceTypeId.ToString()+",'"+s.serviceTypeTitle+"'," +
                                           s.isPayable.ToString()+",'"+s.buildingTitles +"') ";
                                    }
                                    else
                                    {
                                        text += " update ServicesD set serviceStatusId="+ s.serviceStatusId.ToString()+",serviceStatusTitle='" + s.serviceStatusTitle+ "', isPayable="+ s.isPayable.ToString()+
                                            " where serviceId="+ s.id.ToString();
                                    }
                                }
                            }
                            if (text != "")
                            {
                                SqlCommand comm = new SqlCommand();
                                comm.CommandText = text;
                                comm.Connection = conn;
                                try
                                {
                                    int jj = comm.ExecuteNonQuery();
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("Can't write Services" + ": " + ex.Message);
                                    sw.WriteLine(DateTime.Now.ToString() + "    ERR write Services" + ex.Message);
                                }
                            }

                        }
                        row = ss.nextRow;
                        Console.WriteLine("next =" + row);
                        p = GetObjects("https://sud-api.domyland.ru/services?fromRow=" + row.ToString(), token);
                        //cust = cust.Replace("'", "\"").Replace("null", "0"); 
                        p = p.Replace("null", "0");
                         ss = JsonConvert.DeserializeObject<Services>(p);
                    }
                }
            }
        }
        static void GetCustomers(string token,int app=0)
        {
            int row = 0;
            string list = "";
            string cust= GetObjects("https://sud-api.domyland.ru/customers?fromRow=" + row.ToString(), token);
            //cust = cust.Replace("'", "\"").Replace("null", "0"); 
            cust = cust.Replace("null", "0");
            Customers customers = JsonConvert.DeserializeObject<Customers>(cust);
            using (SqlConnection conn = GetConnection(app))
            {
                if (conn != null)
                {
                    while (customers != null && customers.items.Length > 0)
                    {

                        if (conn == null) GetConnection(app);
                        if (conn != null)
                        {
                            string text = "";
                            foreach(Customer c in customers.items)
                            {
                                if (!isUserInList(c.id.ToString().Trim(), list))
                                {
                                    if (list.Length > 0) list += ";";
                                    list += c.id.ToString().Trim();
                                    Boolean isCust = IsExistObject("customerId", c.id.ToString(), "Customers", conn);
                                    string decFirstActivity = c.firstActivity.ToString();
                                    string decLastActivity = c.lastActivity.ToString();
                                    if (c.firstActivity.ToString() == "false") decFirstActivity = "null";
                                    if (c.lastActivity.ToString() == "false") decLastActivity = "null";

                                    if (!isCust)
                                    {
                                        text += " insert into Customers (customerId,extId,customerTypeId,firstName,lastName,middleName,phoneNumber,fullName,shortName,email,INN,SNILS,image,"+
                                            "firstActivity,lastActivity,birthDate,debtSum,entranceNumbers,floors,updatedAt,places,accountNumbers) " +
                                            " values ("+c.id.ToString()+",'"+c.extId+"',"+c.customerTypeId.ToString()+",'"+c.firstName.Replace("'", "\"") + "','"+c.lastName.Replace("'", "\"") + "','"+
                                            c.middleName.Replace("'", "`") + "','" +c.phoneNumber +"','"+c.fullName.Replace("'", "\"") + "','"+c.shortName.Replace("'", "\"") + "','"+c.email+"','"+c.INN+"','"+c.SNILS+"','"+c.image+"',"+
                                           decFirstActivity + ","+ decLastActivity + ",'"+c.birthDate+"',"+c.debtSum.ToString().Replace(",",".")+",'"
                                            +c.entranceNumbers+"','"+c.floors + "',convert(decimal, format(convert(datetime, '" + c.updatedAt.ToString() + "'),'yyyyMMdd')),'" + c.places.Replace("'", "`") + "','"+c.accountNumbers.Replace("'", "`") + "' ) ";
                                    }
                                    else
                                    {
                                        text += " update Customers set email='" + c.email + "',INN='"+c.INN+ "',SNILS='"+c.SNILS+ "',lastActivity="+ decLastActivity + ", "+
                                            " birthDate='"+c.birthDate+ "', debtSum="+c.debtSum.ToString().Replace(",", ".")+ ",updatedAt=convert(decimal, format(convert(datetime, '" + c.updatedAt.ToString() + "'),'yyyyMMdd')) where customerId=" + c.id.ToString()+" ";
                                    }
                                }
                            }
                            if (text != "")
                            {
                                SqlCommand comm = new SqlCommand();
                                comm.CommandText = text;
                                comm.Connection = conn;
                                try
                                {
                                    int jj = comm.ExecuteNonQuery();
                                }
                                catch(Exception ex)
                                {
                                    Console.WriteLine("Can't write Customers" + ": " + ex.Message);
                                    sw.WriteLine(DateTime.Now.ToString() + "    ERR write Customers" + ex.Message);
                                }
                            }
                           
                        }
                        row = customers.nextRow;
                        Console.WriteLine("next =" + row);

                        cust = GetObjects("https://sud-api.domyland.ru/customers?fromRow=" + row.ToString(), token);
                        //cust = cust.Replace("'", "\"").Replace("null", "0");
                        cust = cust.Replace("null", "0");
                        customers = JsonConvert.DeserializeObject<Customers>(cust);
                    }
                }

            }


        }
        static void GetOrdersUpDatesAt(string token,int app=0)
        {
            int rown = 0;
            string listuser = "";
            string listoe = "";
            //string listch = "";
           // string listou = "";
            DateTime today = DateTime.Today;
            //string url = "https://sud-api.domyland.ru/orders?updatedAt=01.01.2023-26.01.2023&fromRow=" + rown.ToString();
            //string url = "https://sud-api.domyland.ru/orders?solveTimeSLA=" + "02.08.2023-03.08.2023" + "&fromRow=" + rown.ToString();
            //string url = "https://sud-api.domyland.ru/orders/3427792";

            string url = "https://sud-api.domyland.ru/orders?updatedAt=" + today.AddDays(dataFinish).ToString("dd.MM.yyyy") + "-" + today.AddDays(dataStart).ToString("dd.MM.yyyy") + "&fromRow="  + rown.ToString();
            // string url = "https://sud-api.domyland.ru/orders?updatedAt=" + today.AddDays(-320).ToString("dd.MM.yyyy") + "-" + today.AddDays(0).ToString("dd.MM.yyyy") + "&fromRow=" + rown.ToString();
            //string url = "https://sud-api.domyland.ru/orders?createdAt=" + today.AddDays(-28).ToString("dd.MM.yyyy") + "-" + today.AddDays(-26).ToString("dd.MM.yyyy") + "&fromRow=" + rown.ToString();
            string orders = "";
            try
            {
                orders = GetObjects(url, token);
                orders = orders.Replace("null", "0");
            }
            catch(Exception ex)
            {
                Console.WriteLine("Err service read" + ": " + ex.Message);
                sw.WriteLine(DateTime.Now.ToString() + "    ERR service read" + ex.Message);
                orders = "";
            }
            
            Orders ord = null;
            //Order order = null;
            if (orders != "")
                        ord= JsonConvert.DeserializeObject<Orders>(orders);
            //if (ord.items == null)
            //    order = JsonConvert.DeserializeObject<Order>(orders);

            string text = "";
            if(ord!=null)
            using (SqlConnection conn = GetConnection(app))
            {
                if (conn != null)

                    while (ord!=null && ord.items.Length > 0)
                    {

                        if (conn == null) GetConnection(app);
                        if (conn != null)
                        {
                            text = "";
                            foreach (Order order in ord.items)
                            {
                                Responsibleuser[] user = order.responsibleUsers;
                                    OrderElement[] oe = order.orderElements;
                                string orderid = order.id.ToString();
                                  //  if (orderid == "1826273")
                                    //    Console.WriteLine("!");

                                Boolean isEx = IsExistObject("orderId", orderid, "Orders", conn);
                                    //if (orderid == "3427792")
                                    //{
                                    //    Console.WriteLine(orderid); Console.Read();

                                    //}
                                    int isAccident = 0;
                                    int isPaidByLK = 0;
                                    int hasOrderRefundWarning = 0;
                                    int isManagementContractSigned = 0;
                                    int isAcceptanceCertificateSigned = 0;
                                    if (order.isAccident.ToString() == "true") isAccident = 1;
                                    if (order.isPaidByLK.ToString() == "true") isPaidByLK = 1;
                                    if (order.hasOrderRefundWarning.ToString() == "true") hasOrderRefundWarning = 1;
                                    if (order.isManagementContractSigned.ToString() == "true") isManagementContractSigned = 1;
                                    if (order.isAcceptanceCertificateSigned.ToString() == "true") isAcceptanceCertificateSigned = 1;
                                    var sourceTitle = order.sourceTitle == null ? " " + "','" : order.sourceTitle.Replace("'", "`").Replace("\"", "`") + "','";
                                    var orderSourceTitle = order.orderSourceTitle == null ? " " + "','" : order.orderSourceTitle.Replace("'", "`").Replace("\"", "`") + "','";
                                    var orderTypeTitle = order.orderTypeTitle == null ? " "  + "'," : order.orderTypeTitle.Replace("'", "`").Replace("\"", "`") + "',";
                                    var orderIssueTypeTitle = order.orderIssueTypeTitle == null ? " " + "','" : order.orderIssueTypeTitle.Replace("'", "`").Replace("\"", "`") + "','";
                                    var serviceTypeTitle = order.serviceTypeTitle == null ? " " + "','" : order.serviceTypeTitle.Replace("'", "`").Replace("\"", "`") + "','";
                                    var serviceTypeImage = order.serviceTypeImage == null ? " " + "'," : order.serviceTypeImage.Replace("'", "`").Replace("\"", "`") + "',";
                                    var createdByUser = order.createdByUser == null ? ""  + "'," : order.createdByUser.Replace("'", "`").Replace("\"", "`") + "',";
                                    var buildingTitle = order.buildingTitle == null ? "" + "'," : order.buildingTitle.Replace("'", "`").Replace("\"", "`") + "',";
                                    var placeTitle = order.placeTitle == null ? "" + "','" : order.placeTitle.Replace("'", "`").Replace("\"", "`") + "','";
                                    var placeNumber = order.placeNumber == null ? ""  + "','" : order.placeNumber.Replace("'", "`").Replace("\"", "`") + "','";
                                    var placeAddress = order.placeAddress == null ? ""  + "'," : order.placeAddress.Replace("'", "`").Replace("\"", "`") + "',";
                                    var placeEntranceNumber = order.placeEntranceNumber == null ? "" + "','" : order.placeEntranceNumber.ToString().Replace(",", ".") + "','";
                                    var accountNumber = order.accountNumber == null ? "" + "','" : order.accountNumber.Replace("'", "`").Replace("\"", "`") + "','";
                                    var placeTypeTitle = order.placeTypeTitle == null ? ""  + "','" : order.placeTypeTitle.Replace("'", "`").Replace("\"", "`") + "','";
                                    var placeTypeShortTitle = order.placeTypeShortTitle == null ? ""  + "'," : order.placeTypeShortTitle.Replace("'", "`").Replace("\"", "`") + "',";
                                    var customerFullName = order.customerFullName == null ? ""  + "','" : order.customerFullName.Replace("'", "`").Replace("\"", "`") + "','";
                                    var customerShortName = order.customerShortName == null ? ""  + "','" : order.customerShortName.Replace("'", "`").Replace("\"", "`") + "','";
                                    var customerPhoneNumber = order.customerPhoneNumber == null ? ""  + "','" : order.customerPhoneNumber.Replace("'", "`").Replace("\"", "`") + "','";
                                    var customerImage = order.customerImage == null ? ""  + "'," : order.customerImage.Replace("'", "`").Replace("\"", "`") + "',";
                                    var customerSummary = order.customerSummary == null ? ""  + "','" : order.customerSummary.Replace("'", "`").Replace("\"", "`") + "','";
                                    var customerStatus = order.customerStatus == null ? ""  + "'," : order.customerStatus.Replace("'", "`").Replace("\"", "`") + "','";
                                    var orderStatusTitle = order.orderStatusTitle == null ? ""  + "','" : order.orderStatusTitle.Replace("'", "`").Replace("\"", "`") + "','";
                                    var acceptedOrderUser = order.acceptedOrderUser == null ? ""  + "'," : order.acceptedOrderUser.Replace("'", "`").Replace("\"", "`") + "',";
                                    var acceptedOrderUserFullName = order.acceptedOrderUserFullName == null ? ""  + "'," : order.acceptedOrderUserFullName.Replace("'", "`").Replace("\"", "`") + "',";
                                    var workOrderUserFullName = order.workOrderUserFullName == null ? ""  + "','" : order.workOrderUserFullName.Replace("'", "`").Replace("\"", "`") + "','";
                                    var closeOrderUserFullName = order.closeOrderUserFullName == null ? ""  + "','" : order.closeOrderUserFullName.Replace("'", "`").Replace("\"", "`") + "','";
                                    var historyStatus = order.historyStatus == null ? ""  + "','" : order.historyStatus.Replace("'", "`").Replace("\"", "`") + "','";
                                    var performersAllStatuses = order.performersAllStatuses == null ? ""  + "','" : order.performersAllStatuses.Replace("'", "`").Replace("\"", "`") + "','";
                                    var managerSubGroup = order.managerSubGroup == null ? ""  + "'," : order.managerSubGroup.Replace("'", "`").Replace("\"", "`") + "',";

                                    if (isEx)
                                    {
                                        text += " update Orders set  orderStatusComment='" + order.orderStatusComment.Replace("'", "`").Replace("\"", "`") + "',updatedAt=" + order.updatedAt.ToString().Replace(",", ".") + ",creditSum=" + order.creditSum.ToString().Replace(",", ".") + ",paidSum=" +
                                           order.paidSum.ToString().Replace(",", ".") + ",refundSum=" + order.refundSum.ToString().Replace(",", ".") + ",totalSum=" + order.totalSum.ToString().Replace(",", ".") + ",invoiceStatusId=" + order.invoiceStatusId.ToString().Replace(",", ".") + ",isPaidByLK=" +
                                           isPaidByLK.ToString().Replace(",", ".") + ",orderRefundSum=" + order.orderRefundSum.ToString().Replace(",", ".") + ",orderRefundStatusId=" + order.orderRefundStatusId.ToString().Replace(",", ".") + ",hasOrderRefundWarning=" + hasOrderRefundWarning +
                                           ",customerMainDebt=" + order.customerMainDebt.ToString().Replace(",", ".") + ",customerCapitalRepairDebt=" + order.customerCapitalRepairDebt.ToString().Replace(",", ".") + ",orderStatusId=" + order.orderStatusId.ToString().Replace(",", ".") +
                                           ",orderStatusTitle='" + order.orderStatusTitle.Replace("'", "`").Replace("\"", "`") + "',orderStatusColor='" + order.orderStatusColor + "',orderStatusColorName='" + order.orderStatusColorName + "',customerDebt=" + order.customerDebt.ToString().Replace(",", ".") +
                                           ",isManagementContractSigned=" + isManagementContractSigned.ToString().Replace(",", ".") + ",closeOrderUser='" + order.closeOrderUser + "',closeOrderAt=" + order.closeOrderAt.ToString().Replace(",", ".") + ",closeOrderUserFullName='" +
                                           order.closeOrderUserFullName + "',historyStatus='" + order.historyStatus.Replace("'", "`").Replace("\"", "`") + "' , performersAllStatuses='" + order.performersAllStatuses.Replace("'", "`").Replace("\"", "`").Replace("\"", "`") + "',serviceTypeTitle='" + order.serviceTypeTitle + "'" +
                                           ",serviceTitle='" + order.serviceTitle + "',placeEntranceNumber='" + order.placeEntranceNumber.ToString().Replace(",", ".") + "'" +
                                           ", buildingTitle='" + order.buildingTitle + "',customerSummary='" + order.customerSummary.Replace("'", "`").Replace("\"", "`") + "',placeAddress='" + order.placeAddress + "'" +
                                           ",rating=" + order.rating.ToString().Replace(",", ".") + ", sourceTitle='" + order.sourceTitle + "', customerId=" + order.customerId.ToString().Replace(",", ".") +
                                           ",placeTypeShortTitle='" + order.placeTypeShortTitle + "',placeNumber='" + order.placeNumber + "',customerShortName='" + order.customerShortName + "',workOrderUserFullName='" + order.workOrderUserFullName +
                                           "',buildingExtId='" + order.buildingExtId + "' where orderId=" + orderid;


                                    }
                                    else
                                    {
                                        text += " insert into Orders (orderId,rating,ratingComment,isAccident,issueId," +
                                            "sourceTitle," +
                                            "orderSourceTitle," +
                                            "orderStatusComment," +
                                            "solveTimeSLA,solveTimeFact, reactionTimeSLA,reactionTimeFact,orderTypeId," +
                                            "orderTypeTitle," +
                                            "orderIssueTypeId," +
                                            "orderIssueTypeTitle," +
                                            "serviceTitle, userWarning,serviceTypeId," +
                                            "serviceTypeTitle," +
                                            "serviceTypeImage," +
                                            "createdAt,updatedAt," +
                                            "createdByUser," +
                                            "creditSum,paidSum,refundSum,totalSum,invoiceStatusId,isPaidByLK,unregisteredCustomerName," +
                                            "unregisteredPhoneNumber,unregisteredEmail,unregisteredAddress,orderRefundSum,orderRefundStatusId,hasOrderRefundWarning,buildingId," +
                                            "buildingExtId," +
                                            "buildingTitle," +
                                            "placeId," +
                                            "placeExtId," +
                                            "placeTitle," +
                                            "placeNumber," +
                                            "placeAddress," +
                                            "placeFloor," +
                                            "placeEntranceNumber," +
                                            "accountNumber," +
                                            "placeTypeTitle," +
                                            "placeTypeShortTitle," +
                                            "customerId,customerFullName,customerShortName,customerPhoneNumber,customerImage," +
                                            "customerTypeId," +
                                            "customerSummary,customerStatus," +
                                            "customerMainDebt,customerCapitalRepairDebt,orderStatusId," +
                                            "orderStatusTitle," +
                                            "orderStatusColor," +
                                            "orderStatusColorName," +
                                            "customerDebt,isManagementContractSigned," +
                                            "isAcceptanceCertificateSigned," +
                                            "acceptedOrderUser," +
                                            "acceptedOrderAt," +
                                            "acceptedOrderUserFullName," +
                                            "workOrderAt," +
                                            "workOrderUserFullName," +
                                            "closeOrderUser," +
                                            "closeOrderAt," +
                                            "closeOrderUserFullName,historyStatus,performersAllStatuses,managerSubGroup," +
                                            "unviewedCommentsCount)" +
                                            " values(" +
                                            order.id.ToString().Replace(",", ".") + "," +
                                            order.rating.ToString().Replace(",", ".") + ",'" +
                                            order.ratingComment.Replace("'", "`").Replace("\"", "`") + "'," +
                                            isAccident.ToString().Replace(",", ".") + "," +
                                            order.issueId.ToString().Replace(",", ".") + ",'" +
                                            sourceTitle +
                                            orderSourceTitle +
                                            order.orderStatusComment.Replace("'", "`").Replace("\"", "`") + "'," +
                                            order.solveTimeSLA.ToString().Replace(",", ".") + "," +
                                            order.solveTimeFact.ToString().Replace(",", ".") + "," +
                                            order.reactionTimeSLA.ToString().Replace(",", ".") + "," +
                                            order.reactionTimeFact.ToString().Replace(",", ".") + "," +
                                            order.orderTypeId.ToString().Replace(",", ".") + ",'" +
                                            orderTypeTitle +
                                            order.orderIssueTypeId.ToString().Replace(",", ".") + ",'" +
                                            orderIssueTypeTitle +
                                            order.serviceTitle.Replace("'", "`").Replace("\"", "`") + "'," +
                                            order.userWarning.ToString().Replace(",", ".") + "," +
                                            order.serviceTypeId.ToString().Replace(",", ".") + ",'" +
                                            serviceTypeTitle +
                                            serviceTypeImage +
                                            order.createdAt.ToString().Replace(",", ".") + "," +
                                            order.updatedAt.ToString().Replace(",", ".") + ",'" +
                                            createdByUser +
                                            order.creditSum.ToString().Replace(",", ".") + "," +
                                            order.paidSum.ToString().Replace(",", ".") + "," +
                                            order.refundSum.ToString().Replace(",", ".") + "," +
                                            order.totalSum.ToString().Replace(",", ".") + "," +
                                            order.invoiceStatusId.ToString().Replace(",", ".") + "," +
                                            isPaidByLK.ToString().Replace(",", ".") + ",'" +
                                            order.unregisteredCustomerName.Replace("'", "`").Replace("\"", "`") + "','" +
                                            order.unregisteredPhoneNumber.Replace("'", "`").Replace("\"", "`") + "','" +
                                            order.unregisteredEmail.Replace("'", "`").Replace("\"", "`") + "','" +
                                            order.unregisteredAddress.Replace("'", "`").Replace("\"", "`") + "'," +
                                            order.orderRefundSum.ToString().Replace(",", ".") + "," +
                                            order.orderRefundStatusId.ToString().Replace(",", ".") + "," +
                                            hasOrderRefundWarning + "," +
                                            order.buildingId.ToString().Replace(",", ".") + ",'" +
                                            order.buildingExtId + "','" +
                                            buildingTitle +
                                            order.placeId.ToString().Replace(",", ".") + ",'" +
                                            order.placeExtId + "','" +
                                            placeTitle +
                                            placeNumber +
                                            placeAddress +
                                            order.placeFloor.ToString().Replace(",", ".") + ",'" +
                                            placeEntranceNumber +
                                            accountNumber +
                                            placeTypeTitle +
                                            placeTypeShortTitle +
                                            order.customerId.ToString().Replace(",", ".") + ",'" +
                                            customerFullName +
                                            customerShortName +
                                            customerPhoneNumber +
                                            customerImage +
                                            order.customerTypeId.ToString().Replace(",", ".") + ",'" +
                                            customerSummary +
                                            customerStatus +
                                            order.customerMainDebt.ToString().Replace(",", ".") + "','" +
                                            order.customerCapitalRepairDebt.ToString().Replace(",", ".") + "','" +
                                            order.orderStatusId.ToString().Replace(",", ".") + "','" +
                                            orderStatusTitle +
                                            order.orderStatusColor + "','" +
                                            order.orderStatusColorName + "','" +
                                            order.customerDebt.ToString().Replace(",", ".") + "','" +
                                            isManagementContractSigned.ToString().Replace(",", ".") + "','" +
                                            isAcceptanceCertificateSigned.ToString().Replace(",", ".") + "','" +
                                            acceptedOrderUser +
                                            order.acceptedOrderAt.ToString().Replace(",", ".") + ",'" +
                                            acceptedOrderUserFullName +
                                            order.workOrderAt.ToString().Replace(",", ".") + ",'" +
                                            workOrderUserFullName +
                                            order.closeOrderUser + "'," +
                                            order.closeOrderAt.ToString().Replace(",", ".") + ",'" +
                                            closeOrderUserFullName +
                                            historyStatus +
                                            performersAllStatuses +
                                            managerSubGroup +
                                            order.unviewedCommentsCount.ToString().Replace(",", ".") +
                                           ") ";
                                        for (int i = 0; i < user.Length; i++)
                                    {
                                        if (!isUserInList(user[i].id.ToString().Trim(), listuser))
                                        {
                                            if (listuser.Length > 0) listuser += ";";
                                            listuser += user[i].id.ToString().Trim();
                                            Boolean isUserE = IsExistObject("userId", user[i].id.ToString().Trim(), "Users", conn);
                                            if (isUserE == false)
                                            {
                                                text += " insert into Users(userId,firstName,lastName,fullName)values(" + user[i].id.ToString() + ",'" + user[i].firstName + "','" + user[i].lastName + "','" + user[i].fullName + "') ";
                                            }

                                        }


                                       // text += " insert into OrderResponsibleuser (userId,orderId) values (" + user[i].id.ToString() + "," + orderid + ") ";
                                    }

                                }
                                    GetChatMessages(token, orderid,conn,ref text, app);
                                    GetOrderResponsibleUser(orderid, user, conn, ref text, app);
                                    for (int i = 0; i < oe.Length; i++)
                                    {

                                        if (!isUserInList(oe[i].elementId.ToString().Trim(), listoe))
                                        {
                                            if (listoe.Length > 0) listoe += ";";
                                            listoe += oe[i].elementId.ToString().Trim();
                                            Boolean isel = IsExistObject("elementId", oe[i].elementId.ToString().Trim(), "OrderElements", conn, orderid);
                                            if (isel == false)
                                            {
                                                text += " insert into OrderElements (orderId, elementId,valueId,valueTitle,elementTitle) values (" + orderid + "," + oe[i].elementId.ToString().Trim() + "," +
                                                    oe[i].valueId + ",'" + oe[i].valueTitle.Replace("'", "`").Replace("\"","`") + "','" + oe[i].elementTitle.Replace("'", "`").Replace("\"","`") + "') ";
                                            }
                                        }
                                           
                                      

                                    }
                                    listoe = "";
                            }
                            if (text != "")
                            {
                                SqlCommand comm = new SqlCommand();
                                comm.Connection = conn;
                                comm.CommandText = text;
                                try
                                {
                                    int ii = comm.ExecuteNonQuery();
                                }
                                catch (Exception ex)
                                {
                                    
                                        Console.WriteLine("Can't write Orders" + ": " + ex.Message+"text=  "+text);
                                    sw.WriteLine(DateTime.Now.ToString() + "    ERR wrute Orders" + ex.Message+ "text=   "+text);
                                }
                            }
                        }
                       rown = ord.nextRow;
                        Console.WriteLine("next =" + ord.nextRow.ToString());
                            //url = "https://sud-api.domyland.ru/orders?updatedAt=01.01.2023-26.01.2023&fromRow=" + rown.ToString();
                            url = "https://sud-api.domyland.ru/orders?updatedAt=" + today.AddDays(dataFinish).ToString("dd.MM.yyyy") + "-" + today.AddDays(dataStart).ToString("dd.MM.yyyy") + "&fromRow=" + rown.ToString();
                            //    url = "https://sud-api.domyland.ru/orders?updatedAt=" + today.AddDays(-320).ToString("dd.MM.yyyy") + "-" + today.AddDays(0).ToString("dd.MM.yyyy") + "&fromRow=" + rown.ToString();
                            try
                            {
                                orders = GetObjects(url, token);
                                orders = orders.Replace("null", "0");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Err service read" + ": " + ex.Message);
                                sw.WriteLine(DateTime.Now.ToString() + "    ERR service read" + ex.Message);
                                orders = "";
                            }

                           ord = null;
                            if (orders != "")
                                ord = JsonConvert.DeserializeObject<Orders>(orders);
                           
                    }
                text = "update Orders set solveTimeSLAD=case when solveTimeSLA=0 then null else dateadd(hour,3,dateadd(S, [solveTimeSLA], '1970-01-01 00:00:00')) end " +
                        " update Orders set solveTimeFactD =case when solveTimeFact = 0 then null else dateadd(hour, 3, dateadd(S, [solveTimeFact], '1970-01-01 00:00:00')) end " +
                        " update Orders set reactionTimeSLAD =case when reactionTimeSLA = 0 then null else dateadd(hour, 3, dateadd(S, [reactionTimeSLA], '1970-01-01 00:00:00')) end " +
                        "  update Orders set reactionTimeFactD =case when reactionTimeFact = 0 then null else dateadd(hour, 3, dateadd(S, [reactionTimeFact], '1970-01-01 00:00:00')) end " +
                        "  update Orders set createdAtD =case when createdAt = 0 then null else dateadd(hour, 3, dateadd(S, [createdAt], '1970-01-01 00:00:00')) end " +
                        "  update Orders set updatedAtD =case when updatedAt = 0 then null else dateadd(hour, 3, dateadd(S, [updatedAt], '1970-01-01 00:00:00')) end " +
                         " update Orders set acceptedOrderAtD =case when acceptedOrderAt = 0 then null else dateadd(hour, 3, dateadd(S, [acceptedOrderAt], '1970-01-01 00:00:00')) end " +
                        "  update Orders set workOrderAtD =case when workOrderAt = 0 then null else dateadd(hour, 3, dateadd(S, [workOrderAt], '1970-01-01 00:00:00')) end " +
                         " update Orders set closeOrderAtD =case when closeOrderAt = 0 then null else dateadd(hour, 3, dateadd(S, [closeOrderAt], '1970-01-01 00:00:00')) end ";
                    text += " update OrderChatMessages set createdAtD =case when createdAt = 0 then null else dateadd(hour, 3, dateadd(S, [createdAt], '1970-01-01 00:00:00')) end ";
                    text += " update OrderChatMessages set updatedAtD =case when updatedAt = 0 then null else dateadd(hour, 3, dateadd(S, [updatedAt], '1970-01-01 00:00:00')) end ";



                    SqlCommand comm1 = new SqlCommand();
                comm1.Connection = conn;
                comm1.CommandText = text;
                try
                {
                    int ii = comm1.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Can't write Orders" + ": " + ex.Message);
                    sw.WriteLine(DateTime.Now.ToString() + "    ERR wrute Orders" + ex.Message);
                }
            }

        }

        static void GetBuildings(string token, int app=0)
        {
            int rown = 0;
            string list = "";
            string buildings = GetObjects("https://sud-api.domyland.ru/buildings&fromRow=" + rown.ToString(), token);
            Buildings bs = JsonConvert.DeserializeObject<Buildings>(buildings);
            string text = "";
            //
            using (SqlConnection conn = GetConnection(app))
            {
                if (conn != null)

                    while (bs!=null && bs.items.Length > 0)
                    {

                        if (conn == null) GetConnection(app);
                        if (conn != null)
                        {
                            text = "";
                            foreach (Building b in bs.items)
                            {
                                //
                                Console.WriteLine(b.id + " " + b.title + " " + b.address);
                                string buildingId = b.id.ToString();
                                Boolean isEx = IsExistObject("buildingId", buildingId, "Buildings", conn);
                                if (!isUserInList(b.id.ToString().Trim(), list))
                                {
                                    if (list.Length > 0) list += ";";
                                    list += b.id.ToString().Trim();
                                    Boolean isCust = IsExistObject("buildingId", buildingId, "Buildings", conn);
                                    if (!isCust)
                                    {
                                        text += " insert into Buildings (buildingId,extId,title, address,street, houseNumber, entrancesCount, protectedArea,onlinePayment," +
                "companyId,companyTitle,companyLegalName,buildingTypeId,buildingTypeTitle,buildingTypeShortTitle) values ("+b.id.ToString()+",'" +
                b.extId.ToString() + "','" + b.title + "','" + b.address + "','" + b.street + "','" + b.houseNumber + "'," + b.entrancesCount + "," + b.protectedArea + "," + b.onlinePayment + "," + b.companyId + ",'" +
                b.companyTitle + "','" + b.companyLegalName + "'," + b.buildingTypeId + ",'" + b.buildingTypeTitle + "','" + b.buildingTypeShortTitle + "') ";
                                    }
                                    else
                                    {
                                       // text += " udate Buildings set extId='" + b.extId.ToString() + "' where buildingId=" + b.id.ToString();
                                    }
                                   
                                }
                                //

                            }
                      //      Console.ReadKey();

                            if (text != "")
                            {
                                SqlCommand comm = new SqlCommand();
                                comm.Connection = conn;
                                comm.CommandText = text;
                                try
                                {
                                    int ii = comm.ExecuteNonQuery();
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("Can't write Buildings" + ": " + ex.Message);
                                    sw.WriteLine(DateTime.Now.ToString() + "    ERR write Buildings " + ex.Message);
                                }
                            }
                        }
                        rown =bs.nextRow;
                        Console.WriteLine("next =" + bs.nextRow.ToString());

                         buildings = GetObjects("https://sud-api.domyland.ru/buildings&fromRow=" + rown.ToString(), token);
                         bs = JsonConvert.DeserializeObject<Buildings>(buildings);

                    }
            }

        }

        static void GetChatMessages(string token, string orderid, SqlConnection conn, ref string text, int app = 0)
        {
            int rown = 0;
            string list = "";

            string pars = GetObjects("https://sud-api.domyland.ru/chatmessages&orderId=" + orderid + "&fromRow=" + rown.ToString(), token);
            pars = pars.Replace("null", "0").Replace("false", "0").Replace("true", "1").Replace("False", "0").Replace("True", "1");
            ChatMess ps = JsonConvert.DeserializeObject<ChatMess>(pars);
         
            //
            

                    while (ps != null && ps.items.Length > 0)
                    {

                        if (conn == null) GetConnection(app);
                        if (conn != null)
                        {
                          //  text = "";
                            foreach (ChatItem b in ps.items)
                            {
                                //
                                string messId = b.id.ToString();
                                //  Boolean isEx = IsExistObject("messageid", messId, "OrderChatMessages", conn,orderid);
                                if (!isUserInList(b.id.ToString().Trim(), list))
                                {
                                    if (list.Length > 0) list += ";";
                                    list += b.id.ToString().Trim();
                                    Boolean isCust = IsExistObject("messageid", messId, "OrderChatMessages", conn, orderid);
                                    if (!isCust)
                                    {
                                        text += " insert into OrderChatMessages (orderid ,messageid, MessageText, fileUrl,  fileName, fileSize,  fileTypeId," +
"fileOriginalName, chatMessageTypeId,  author,  authorImage,  isViewed ,  isOwnMessage, createdAt, updatedAt, isUpdateAllowed, isDeleteAllowed, createdByUserId , createdByCustomerId) values (" +
orderid + "," + b.id.ToString() + ",'" +
                b.text.Replace("'", "`").Replace("\"", "`") + "','" + b.fileUrl + "','" + b.fileName + "'," + b.fileSize + "," + b.fileTypeId + ",'" + b.fileOriginalName.Replace("'", "`").Replace("\"", "`") + "',"
                + b.chatMessageTypeId + ",'" + b.author.Replace("'", "`").Replace("\"", "`") + "','" + b.authorImage.Replace("'", "`").Replace("\"", "`") + "'," + b.isViewed + "," + b.isOwnMessage + "," + b.createdAt + "," + b.updatedAt + "," + b.isUpdateAllowed +
                "," + b.isDeleteAllowed + "," + b.createdByUserId + "," + b.createdByCustomerId + ") ";

                                    }

                                }
                                else
                                {
                                    text += " update OrderChatMessages set MessageText='" + b.text + "',fileUrl='" + b.fileUrl + "',fileName='" + b.fileName + "',fileSize=" + b.fileSize +
                                        ",fileTypeId=" + b.fileTypeId + ",fileOriginalName='" + b.fileOriginalName.Replace("'", "`").Replace("\"", "`") + "',author='" + b.author.Replace("'", "`").Replace("\"", "`") + "',authorImage='" +
                                        b.authorImage.Replace("'", "`").Replace("\"", "`") + "',isViewed=" + b.isViewed + ",isOwnMessage=" + b.isOwnMessage + ",updatedAt=" + b.updatedAt + ", isUpdateAllowed=" + b.isUpdateAllowed +
                                        ", isDeleteAllowed=" + b.isDeleteAllowed + " where orderid=" + orderid + " and messageid=" + messId;
                                }
                                //


                            }
                           
                        }
                        rown = ps.nextRow;
                        //Console.WriteLine("next =" + ps.nextRow.ToString());
                        pars = GetObjects("https://sud-api.domyland.ru/chatmessages&orderId=" + orderid + "&fromRow=" + rown.ToString(), token);
                pars = pars.Replace("null", "0").Replace("false", "0").Replace("true", "1").Replace("False", "0").Replace("True", "1");
                ps = JsonConvert.DeserializeObject<ChatMess>(pars);

                    }
         
        }
        static void GetOrderResponsibleUser( string orderid, Responsibleuser[] users,SqlConnection conn,ref string text, int app = 0)
        {
            string listuser = "";
            text += " delete from OrderResponsibleuser where orderId=" + orderid;
            for (int i = 0; i < users.Length; i++)
            {
                if (!isUserInList(users[i].id.ToString().Trim(), listuser))
                {
                    if (listuser.Length > 0) listuser += ";";
                    listuser += users[i].id.ToString().Trim();
                  //  Boolean isUserE = IsExistObject("userId", users[i].id.ToString().Trim(), "OrderResponsibleuser", conn,orderid);
                    //  text += " insert into OrderResponsibleuserTest (userId,orderId) values (" + users[i].id.ToString() + "," + orderid + ") ";
                   
                  //  if (isUserE == false)
                    {
                        text += " insert into OrderResponsibleuser (userId,orderId) values (" + users[i].id.ToString() + "," + orderid + ") ";
                       
                    }

                }


              
            }


        }
        static string GetObjects(string url,string token)
        {
            string res = "";
            var request = WebRequest.Create(url);
            request.ContentType = "application/json";
            request.Method = "GET";
            request.Headers["Authorization"] = "Bearer " + token;
            try
            {
                HttpWebResponse response = request.GetResponse() as HttpWebResponse;
                using (Stream responseStream = response.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(responseStream, Encoding.UTF8);
                    res = reader.ReadToEnd();
                }
            }
            catch (Exception ex)
            {
                sw.WriteLine(DateTime.Now.ToString() + "     ERR=" + ex.Message);
              //  Console.ReadKey();

            }
            return res;
        }
        static string GetToken2()
        {
            string result = "";
            RequestToken rt = new RequestToken
            {
                email = "statistics@uk-object.ru",
                password = "gaZ&X4d3"
            };
            string instr = JsonConvert.SerializeObject(rt);
            //"{'email': 'MR-API@vysota-service.ru','password': 'Q5uUMq0Ag0y!6b0qS9L#'}";
            var request = WebRequest.Create("https://sud-api.domyland.ru/auth");

            request.ContentType = "application/json";
            //    request.Headers["Authorization"] = token;

            request.Method = "POST";
            using (var streamWriter = new StreamWriter(request.GetRequestStream()))
            {

                streamWriter.Write(instr);
            }
            try
            {
                HttpWebResponse response = request.GetResponse() as HttpWebResponse;
                using (Stream responseStream = response.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(responseStream, Encoding.UTF8);
                    result = reader.ReadToEnd();
                }
            }
            catch (Exception ex)
            {
                sw.WriteLine(DateTime.Now.ToString() + "     ERR=" + ex.Message);
               // Console.ReadKey();

            }
            return result;
        }

    }
}
