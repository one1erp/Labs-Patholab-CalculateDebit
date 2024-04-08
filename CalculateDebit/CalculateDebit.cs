using System.Data.Entity;
using ADODB;
using LSEXT;
using LSSERVICEPROVIDERLib;
using Microsoft.Win32.SafeHandles;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using Patholab_Common;
using Patholab_DAL_V1;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Diagnostics;//for debugger :)
using System.IO;
using System.Linq;
using System.Management;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Windows.Forms;


namespace CalculateDebit
{

    [ComVisible(true)]
    [ProgId("CalculateDebit.CalculateDebitCls")]
    public class CalculateDebitCls : IWorkflowExtension
    {
        private double sessionId;
        private bool _debug = false;
        private string _connectionString;

        private OracleConnection _connection;

        INautilusServiceProvider sp;
        private DataLayer dal;
        private const string Type = "1";
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern SafeFileHandle CreateFile(string lpFileName, FileAccess dwDesiredAccess,
        uint dwShareMode, IntPtr lpSecurityAttributes, FileMode dwCreationDisposition,
        uint dwFlagsAndAttributes, IntPtr hTemplateFile);

        public void Init(DataLayer dataLayer, OracleConnection connection, bool debug)
        {

            dal = dataLayer;
            _connection = connection;
            debug = debug;
        }

        public void Execute(ref LSExtensionParameters Parameters)
        {
            _debug = false;
            try
            {

                #region params

                string tableName = Parameters["TABLE_NAME"];
                string role = Parameters["ROLE_NAME"];


                _debug = (role.ToUpper() == "DEBUG");

                if (_debug) Debugger.Launch();

                sp = Parameters["SERVICE_PROVIDER"];
                var rs = Parameters["RECORDS"];


                //Recordset rs = Parameters["RECORDS"];

                #endregion

                ////////////יוצר קונקשן//////////////////////////
                var ntlCon = Utils.GetNtlsCon(sp);
                Utils.CreateConstring(ntlCon);
                /////////////////////////////           

                _connection = GetConnection(ntlCon);

           //     MessageBox.Show("0.1.Before new DataLayer");

                dal = new DataLayer();
              

                dal.Connect(ntlCon);
             

                bool debitCreated = false;

                //ashi -debug
              
                //string aq = dal.FindBy<ALIQUOT>(a => a.ALIQUOT_ID == 3123).FirstOrDefault().NAME;
           //     MessageBox.Show("1.After First query from db " + aq);

                #region Aliquot

                if (tableName == "ALIQUOT")
                {
                    rs.MoveLast();

                    //string workstationId = Parameters["WORKSTATION_ID"].ToString();
                    double aliquotId = rs.Fields["ALIQUOT_ID"].Value;

                    ALIQUOT aliquot = dal.FindBy<ALIQUOT>(a => a.ALIQUOT_ID == aliquotId).Include(x => x.SAMPLE)
                    .Include(x => x.SAMPLE.SAMPLE_USER)
                    .Include(x => x.SAMPLE.SDG)
                    .Include(x => x.SAMPLE.SDG.SDG_USER)
                    .Include(x => x.SAMPLE.SDG.SDG_USER.U_ORDER)
                    .Include(x => x.SAMPLE.SDG.SDG_USER.U_ORDER.U_ORDER_USER)
                    .Include(x => x.SAMPLE.SDG.SDG_USER.IMPLEMENTING_PHYSICIAN)
                    .Include(x => x.SAMPLE.SDG.SDG_USER.IMPLEMENTING_PHYSICIAN.SUPPLIER_USER)
                    .Include(x => x.SAMPLE.SDG.SDG_USER.U_ORDER.U_ORDER_USER.U_CUSTOMER1)
                    .SingleOrDefault();
                    debitCreated = RunOnAliquot(aliquot);
                }
                #endregion

                #region Sample
                //else if (tableName == "SAMPLE")
                //{
                //    rs.MoveLast();
                //    rs.MoveLast();
                //    string SdgIdSring = rs.Fields["SDG_ID"].Value.ToString();
                //    long sdgId = long.Parse(SdgIdSring);

                //    string sampleIdSring = rs.Fields["SAMPLE_ID"].Value.ToString();
                //    string workstationId = Parameters["WORKSTATION_ID"].ToString();
                //    long sampleId = long.Parse(SdgIdSring);

                //    debitCreated = CreatedDebitForSample(sdgId, debug, SdgIdSring, sampleIdSring, sampleId, debitCreated);
                //}
                #endregion

                #region SDG

                else if (tableName == "SDG")
                {
                    rs.MoveLast();
                    string SdgIdSring = rs.Fields["SDG_ID"].Value.ToString();
                    //string workstationId = Parameters["WORKSTATION_ID"].ToString();
                    long sdgId = long.Parse(SdgIdSring);

                    //find "part" connected to SDG by   SDG->U_ORDER(s)->COSTUMER->PRICE_LIST_name->PART
                    SDG sdg = dal.FindBy<SDG>(d => d.SDG_ID == sdgId).Include(x => x.SDG_USER)
                                 .Include(x => x.SDG_USER.U_ORDER)
                                 .Include(x => x.SDG_USER.U_ORDER.U_ORDER_USER)
                                 .Include(x => x.SDG_USER.IMPLEMENTING_PHYSICIAN)
                                 .Include(x => x.SDG_USER.IMPLEMENTING_PHYSICIAN.SUPPLIER_USER)
                                 .Include(x => x.SDG_USER.U_ORDER.U_ORDER_USER.U_CUSTOMER1)
                                 .SingleOrDefault();
                    ;
                    debitCreated = RunOnSDG(sdg);
                }
                #endregion

                else
                {
                    if (_debug) MessageBox.Show("זה לא סדג");
                }
                if (!debitCreated)
                {
                    if (_debug) MessageBox.Show("רשומת חיוב לא נוצרה");
                }
                else
                {

                    if (_debug) MessageBox.Show("רשומת חיוב נוצרה בהצלחה");

                }
            }
            catch (Exception ex)
            {
                if (_debug) MessageBox.Show("תקלה ביצירת רשומת חיוב");
                Logger.WriteLogFile(ex);
            }
            finally
            {
                if (dal != null) dal.Close();
                dal = null;
                if (_connection != null)
                {
                    _connection.Close();
                }
                // ashi 15.8.18 for leaking memory
                GC.Collect();
                GC.SuppressFinalize(this);

            }
        }

        public bool RunOnSDG(SDG sdg)
        {
            long sdgId = sdg.SDG_ID;
            string SdgIdSring = sdgId.ToString();
            bool debitCreated = false;
            if ("XR".Contains(sdg.STATUS))
            {
                if (_debug) MessageBox.Show("The SDG is in status A,X or R and cannnot be given a debit");
                return debitCreated;
            }
            //U_ORDER_USER[] orders = dal.FindBy<U_ORDER_USER>(ou => ou.U_SDG_NAME == sdg.NAME && ou.U_STATUS != "X").ToArray();
            U_ORDER_USER order;
            if (sdg.SDG_USER.U_ORDER == null)
            {
                if (_debug) MessageBox.Show("Can't create debit. order not defined");
            }
            else
            {
                order = sdg.SDG_USER.U_ORDER.U_ORDER_USER;
                //    foreach (U_ORDER_USER order in orders)

                {
                    U_CUSTOMER_USER customer =
                        dal.FindBy<U_CUSTOMER_USER>(cu => cu.U_CUSTOMER_ID == order.U_CUSTOMER)
                           .SingleOrDefault(); //=order.U_CUSTOMER1.U_CUSTOMER_USER;// 
                    if (customer == null)
                    {
                        if (_debug) MessageBox.Show("Can't create debit. Customer not defined");
                    }
                    else if (customer.U_PRICE_LIST_NAME == null)
                    {
                        if (_debug)
                            MessageBox.Show("Can't create debit. Price List not defined for Customer " +
                                            customer.U_CUSTOMER.NAME);
                    }
                    else
                    {
                        //		SDG ->	Order			קיים
                        //     Order->Customer        קיים
                        //     Customer->Customer_user       קיים
                        //      Customer_user->u_ u_price_list_name Link    (צריך לבדוק, צריך להיות קיים)
                        //e.צור רשומה ב - Debit       חדש – תמיד הכנס רשומה זאת!
                        //i.עבור    Customer    ועבור   u_order_user.u_part_Code
                        //ii.אם קיימת רשומת מחיר  במחירון(לפי שם מחירון, ופריט ההזמנה)
                        //iii.עם  u_price_list_name and u_part_Code
                        //iv.הכנס מחיר מהרשומה(יכול להיות – 0 או ריק שגם הוא - 0)
                        //v.אחרת – הכנס מחיר(-1) !
                        //f.בכל מקרה הוסף רישום ל – Sdg Log
                        //g.אחר כך המשך לבדוק רשומות אחרות מאותו מחירון u_price_list_name
                        //i.פריטים מאותו סוג    u_Parts_user.u_part_type    כמו סוג פריט ההזמנה
                        //ii.או מסוג Q כללי
                        //iii.רק פריטים שהשאילתא שלהם מחזירה ערך 1        הוסף ל - Debit
                        //iv.כל השאילתות מוגדרות עם פרמטר    Order_id    בלבד.


                        long sessionIdlong = (long)sessionId;

                        U_PARTS_USER[] parts = null;

                        U_PRICE_LIST_USER[] defaultPriceList = null;
                        if (order.U_PARTS_ID != null)
                        {
                            try
                            {
                                // for sdg plu.U_PARTS.U_PARTS_USER.U_PART_TYPE ***!=*** "S"
                                defaultPriceList = dal
                                    .FindBy<U_PRICE_LIST_USER>(
                                        plu => plu.U_PRICE_LIST_NAME.Trim() == customer.U_PRICE_LIST_NAME.Trim()
                                               && plu.U_EFCT_FROM_DATE != null
                                               && plu.U_EFCT_FROM_DATE < DateTime.Today
                                               && plu.U_PARTS.U_PARTS_USER.U_PART_TYPE != "S"
                                    )
                                    .OrderByDescending(plu => plu.U_EFCT_FROM_DATE).ToArray();
                            }
                            catch (Exception ex)
                            {
                                Logger.WriteLogFile(ex);
                                defaultPriceList = null;
                            }
                        }

                        if (defaultPriceList != null && defaultPriceList.Count() != 0)
                        {
                            if (_debug)
                                MessageBox.Show("Calculating debit for price list \"" +
                                                customer.U_PRICE_LIST_NAME + "\"");

                            //U_PARTS_USER defaultPart = order.U_PARTS.U_PARTS_USER;
                            //dal.InsertToSdgLog(sdgId, "SDG.FirstDebitLine", sessionIdlong,
                            //                   order.U_ORDER.NAME + "," + customer.U_CUSTOMER.NAME
                            //                   + "," + customer.U_PRICE_LIST_NAME);

                            ////DealWithPrice(customer, defaultPart,1.0M, order, sdg, SdgIdSring, ref debitCreated, debug);
                            //debitCreated = DebitCreated(defaultPart, debug, SdgIdSring, order, customer, sdg,
                            //                            debitCreated);
                            ////if there is a default price list for custoer, get its price 

                            int i = 0;

                            parts = new U_PARTS_USER[defaultPriceList.Count()];
                            foreach (U_PRICE_LIST_USER uPriceListUser in defaultPriceList)
                            {
                                parts[i++] = uPriceListUser.U_PARTS.U_PARTS_USER;
                            }

                            if (parts.Count() == 0) parts = null;
                        }
                        if (parts == null || defaultPriceList.Count() == 0 || order.U_PARTS_ID == null)
                        {
                            //SDG_LOG test = dal.FindBy<SDG_LOG>(dl => dl.SDG_ID == 611).FirstOrDefault();
                            dal.InsertToSdgLog(sdgId, "SDG.FirstDebitLine", sessionIdlong,
                                               order.U_ORDER.NAME + "," + customer.U_CUSTOMER.NAME
                                               + "," + ""
                                               + "," + "");
                            //if there is no default price list for custoer, get only price from Q group
                            if (_debug)
                            {
                                if (defaultPriceList == null)
                                {
                                    MessageBox.Show("There is no default price list for customer " +
                                                    customer.U_CUSTOMER.NAME + ", get only price from Q group");
                                }
                                if (order.U_PARTS == null)
                                {
                                    MessageBox.Show("There is no default part for Order " + order.U_ORDER.NAME +
                                                    ", get only price from Q group");
                                }
                            }
                            parts = dal.FindBy<U_PARTS_USER>(pu => pu.U_DISPLAY_ON_PRICE_LIST == "T"
                                                                   && pu.U_PART_TYPE == "Q"
                                                                   && pu.U_QUERY != null).ToArray();
                        }
                        if (parts == null || parts.Count() == 0)
                        {
                            if (_debug) MessageBox.Show("Can't create debit. PARTS Missing");
                            //return;
                        }
                        else
                        {
                         //   MessageBox.Show("before DebitCreated Function ");
                            foreach (U_PARTS_USER part in parts)
                            {
                                debitCreated = DebitCreated(part, _debug, SdgIdSring, order, customer, sdg,
                                                            debitCreated);
                            }
                        }
                    }
                }

                foreach (SAMPLE sample in sdg.SAMPLEs.Where(s => s.STATUS != "X" && s.STATUS != "R"))
                {
                    debitCreated = CreatedDebitForSample(sdgId, _debug, SdgIdSring, sample.SAMPLE_ID.ToString(),
                                                         sample.SAMPLE_ID,
                                                         debitCreated);
                }
            }
            return debitCreated;
        }

        public bool RunOnAliquot(ALIQUOT aliquot)
        {
            bool debitCreated = false;
            if ("XR".Contains(aliquot.STATUS))
            {
                if (_debug) MessageBox.Show("The aliquot is in status X or R and cannnot be given a debit");
                return true;
            }

            if (aliquot.ALIQUOT_USER.U_DEBIT != null)
            {
                if (_debug) MessageBox.Show("קיים חיוב לסלייד, לא ניתן לתמחר שוב");
                return debitCreated;
            }
            if (aliquot.ALIQUOT_USER.U_COLOR_TYPE == null)
            {
                if (_debug) MessageBox.Show("לא הוגדרה צביעה לאליקווט, לא ניתן לתמחר");
                return debitCreated;
            }

            U_ORDER_USER order;
            //get the order for the aliquot
            if (aliquot.SAMPLE == null || aliquot.SAMPLE.SDG == null ||
                aliquot.SAMPLE.SDG.SDG_USER.U_ORDER == null)
            {
                if (_debug) MessageBox.Show("Can't create debit. order not defined or the aliquot has no SDG");
            }
            else
            {
                order = aliquot.SAMPLE.SDG.SDG_USER.U_ORDER.U_ORDER_USER;
                {
                    // U_CUSTOMER_USER customer = dal.FindBy<U_CUSTOMER_USER>(cu => cu.U_CUSTOMER_ID == order.U_CUSTOMER).SingleOrDefault();
                    U_CUSTOMER_USER customer;
                    //get the customer for the order
                    if (order.U_CUSTOMER1 == null)
                    {
                        if (_debug) MessageBox.Show("Can't create debit. Customer not defined");
                    }
                    else
                    {
                        customer = order.U_CUSTOMER1.U_CUSTOMER_USER;
                        U_PARTS_USER[] parts = dal
                            .FindBy<U_PARTS_USER>(pu => pu.U_STAIN == aliquot.ALIQUOT_USER.U_COLOR_TYPE)
                            .ToArray();
                        if (parts == null || parts.Count() == 0)
                        {
                            if (_debug) MessageBox.Show("Can't create debit. PARTS Missing");
                            return debitCreated;
                        }
                        else
                            foreach (U_PARTS_USER part in parts)
                            {
                                decimal? nullablePrice = null;
                                bool isUrgent = false;
                                // check for price or Run_GET_PRICE_URGENT
                                if (aliquot.SAMPLE.SDG.SDG_USER.U_PRIORITY == 2)
                                {
                                    isUrgent = true;
                                    nullablePrice = dal.Run_GET_PRICE_URGENT(customer.U_CUSTOMER_ID,
                                                                             part.U_PARTS_ID);
                                    //if no price for urgen, get normal price
                                    if (nullablePrice == null)
                                    {
                                        nullablePrice = dal.Run_GET_PRICE(customer.U_CUSTOMER_ID,
                                                                              part.U_PARTS_ID);
                                        isUrgent = false;
                                    }

                                }
                                else
                                {
                                    nullablePrice = dal.Run_GET_PRICE(customer.U_CUSTOMER_ID,
                                                                      part.U_PARTS_ID);
                                }
                                string groupText = "";

                                //The active part can be a part for a single or a group patrt
                                U_PARTS_USER activePart = part;
                                if (nullablePrice == null)
                                {
                                    // IF there is no price, check for price for the group, and create debit for the group part
                                    //assuming a single group price
                                    activePart =
                                        dal.FindBy<U_PARTS_USER>(
                                            pu => pu.U_GRP_CODE == part.U_GRP_CODE && pu.U_PART_TYPE == "G")
                                           .SingleOrDefault();
                                    //roy : dealing with no active part
                                    if (activePart == null)
                                    {
                                        nullablePrice = null;

                                    }
                                    else
                                    {
                                        if (aliquot.SAMPLE.SDG.SDG_USER.U_PRIORITY == 2)
                                        {
                                            nullablePrice = dal.Run_GET_PRICE_URGENT(customer.U_CUSTOMER_ID,
                                                                                     activePart.U_PARTS_ID);
                                            isUrgent = true;
                                            if (nullablePrice == null)
                                            {
                                                nullablePrice = dal.Run_GET_PRICE(customer.U_CUSTOMER_ID,
                                                                                activePart.U_PARTS_ID);
                                                isUrgent = false;
                                            }

                                        }
                                        else
                                        {
                                            nullablePrice = dal.Run_GET_PRICE(customer.U_CUSTOMER_ID,
                                                                              activePart.U_PARTS_ID);
                                        }
                                        groupText = "" + activePart.U_GRP_NAME + "";
                                    }
                                }
                                if (nullablePrice == null)
                                {
                                    // IF there is no GRUOP price, check for price for the group, and create debit
                                    if (_debug) MessageBox.Show("Can't create debit. No Price for stain");
                                    //return;
                                    debitCreated = true;
                                }
                                else
                                {
                                    // IF there is a price, create debit
                                    decimal price = (decimal)nullablePrice;
                                    decimal priceIncludingVAT;
                                    PHRASE_HEADER Params = dal.GetPhraseByName("System Parameters");
                                    string vatString;
                                    Params.PhraseEntriesDictonary.TryGetValue("Vat Precent", out vatString);
                                    vatString = vatString.Replace("%", "");


                                    decimal vat = Convert.ToDecimal(vatString);
                                    priceIncludingVAT = price * (vat / 100 + 1);
                                    //if (customer.U_INC_VAT == "T")
                                    //{
                                    //    try
                                    //    {

                                    //        priceIncludingVAT = price;
                                    //        price = price/(vat/100 + 1);
                                    //    }
                                    //    catch
                                    //    {
                                    //        if (_debug)
                                    //            MessageBox.Show(
                                    //                @"Error, Debit not created. Could not find entry ""Vat Precent"" in Phrase ""Params""");
                                    //        return debitCreated;
                                    //    }
                                    //}
                                    //else
                                    //{
                                    //    priceIncludingVAT = price*(vat/100 + 1);
                                    //}
                                    //check if debit exits for this part

                                    //28.06.18//Ashi - אם הזמן הנוכחי שונה מהזמן של הרשומה אז להוסיף שורת חיוב חדשה
                                    var month = System.DateTime.Now.Month;
                                    Debugger.Launch();
                                    U_DEBIT_USER debit =
                                        dal.FindBy<U_DEBIT_USER>(
                                            du =>
                                            du.U_SDG_NAME == aliquot.SAMPLE.SDG.NAME &&
                                            du.U_PARTS_ID == activePart.U_PARTS_ID
                                            && (du.U_EVENT_DATE.HasValue && du.U_EVENT_DATE.Value.Month == month)
                                            && du.U_DEBIT_STATUS != "X"
                                            && du.U_DEBIT_STATUS != "C")
                                            .SingleOrDefault();
                                    if (debit != null)
                                    {
                                        //if debit exits, check if the debit contains the current aliquot name
                                        if (debit.U_ENTITY_ID.Contains(aliquot.NAME + " - "))
                                        {
                                            //do nothing, if aliquot in the debit
                                            if (_debug)
                                                MessageBox.Show("Can't create debit for part '" +
                                                                activePart.U_PARTS.NAME + "', stain '" +
                                                                aliquot.ALIQUOT_USER.U_COLOR_TYPE +
                                                                "' and aliquot " + aliquot.NAME +
                                                                ". Debit already exists");
                                            // return;
                                            debitCreated = true;
                                        }
                                        else
                                        {
                                            //if aliquot is not in the debit, add it
                                            debit.U_ENTITY_ID += ", " + aliquot.NAME + " - " +
                                                                 aliquot.ALIQUOT_USER.U_COLOR_TYPE;
                                            if (isUrgent) debit.U_ENTITY_ID += "*";
                                            debit.U_QUANTITY += 1;
                                            debit.U_PART_PRICE = Round3(price);
                                            debit.U_PRICE_INC_VAT = Round3(priceIncludingVAT); //price per unit inc vat

                                            debit.U_LINE_AMOUNT = Round3(debit.U_QUANTITY * debit.U_PRICE_INC_VAT);
                                            //part text is : "stain name" or "(group)stain name"
                                            // debit.U_PART_TEXT += ","+aliquot.ALIQUOT_USER.U_COLOR_TYPE ;

                                            string uColorType = aliquot.ALIQUOT_USER.U_COLOR_TYPE;
                                            if (isUrgent) uColorType += " דחוף";
                                            debit.U_DEBIT.DESCRIPTION += "," + uColorType;

                                            debit.U_LAST_UPDATE = aliquot.CREATED_ON;
                                            dal.InsertToSdgLog(aliquot.SAMPLE.SDG.SDG_ID, "ALIQUOT.Add Debit",
                                                               (long)sessionId,
                                                               aliquot.NAME + "," + activePart.U_PARTS.NAME +
                                                               "(" + uColorType + ")," +
                                                               price);
                                            aliquot.ALIQUOT_USER.U_DEBIT_ID = debit.U_DEBIT_ID;
                                            dal.SaveChanges();
                                            debitCreated = true;
                                        }
                                    }
                                    else
                                    {
                                        //if debit does not exit, craete a debit for the current Part, with the current aliquot name as the entity name, with quantity=1
                                        string entityName = aliquot.NAME + " - " + aliquot.ALIQUOT_USER.U_COLOR_TYPE;
                                        if (isUrgent) entityName += "*";
                                        string debitDescription = aliquot.ALIQUOT_USER.U_COLOR_TYPE + " דחוף";
                                        aliquot.ALIQUOT_USER.U_DEBIT_ID = InsertDebit(ref debitCreated,
                                                    entityName,
                                                    activePart.U_PARTS.NAME, aliquot.SAMPLE.SDG, order,
                                                    activePart, 1.0M, price, priceIncludingVAT, "ALIQUOT",
                                                    debitDescription, aliquot.CREATED_ON);

                                        dal.SaveChanges();
                                        debitCreated = true;
                                        //this " - " is needed to  find the aliquot ----^ DON'T DELETE
                                    }
                                }
                            }
                    }
                }
            }
            return debitCreated;
        }

        private bool DebitCreated(U_PARTS_USER part, bool debug, string SdgIdSring, U_ORDER_USER order, U_CUSTOMER_USER customer,
                                  SDG sdg, bool debitCreated)
        {


            object value;
            // get query in U_QUERY

            string query = part.U_QUERY;
            if (string.IsNullOrEmpty(query))
            {
                value = null;
                if (debug)
                    MessageBox.Show("Query for PART #" + part.U_PARTS.NAME + " is empty\r\n");
            }
            else if (part.U_DISPLAY_ON_PRICE_LIST != "T")
            {
                if (debug)
                    MessageBox.Show("PART #" + part.U_PARTS.NAME +
                                    " is not calculated, U_DISPLAY_ON_PRICE_LIST is false\r\n");
                value = null;
            }
            else
            {
                // replace #SDG_ID# and other fielsd in U_QUERY
                query = MyReplace(query, "#SDG_ID#", SdgIdSring);

                query = MyReplace(query, "#ORDER_ID#", order.U_ORDER_ID.ToString());
                query = MyReplace(query, "#SAMPLE_ID#", "0");
                query = MyReplace(query, "#CUSTOMER_ID#", customer.U_CUSTOMER_ID.ToString());
                query = MyReplace(query, "#PRICE_LEVEL#", customer.U_PRICE_LEVEL ?? "");
                query = MyReplace(query, "#PART_CODE#", order.U_PARTS.U_PARTS_USER.U_PART_CODE ?? "");
                query = MyReplace(query, "\r", " ");
                query = MyReplace(query, "\n", " ");
                // dal.RunQuery(query);
                OracleCommand cmd = new OracleCommand(query, _connection);


                try
                {
                    value = cmd.ExecuteScalar();

                }
                catch (Exception ex)
                {
                    if (debug)
                        MessageBox.Show("Error running query for PART #" + part.U_PARTS.NAME +
                                        "\r\n" + ex);
                    value = null;
                    //continue loop 
                }
                finally
                {

                    cmd.Dispose();
                    cmd = null;
                }


                // Run query in U_QUERY, 
            }
            if (value == null)
            {
                if (debug) MessageBox.Show("Can't create debit. PARTS query for PART #" + part.U_PARTS.NAME + " returned null");
                //return;
            }
            else
            {
                try
                {

                    decimal? quantity = decimal.Parse(value.ToString());

                    //If Qery returns true, get price

                    DealWithPrice(customer, part, quantity, order, sdg, SdgIdSring, ref debitCreated, debug);
                    //}
                }
                catch (Exception ex)
                {
                    if (debug)
                        MessageBox.Show("Error Parsing value to quantity for part :'" + part.U_PARTS.NAME +
                                        "'");
                    Logger.WriteLogFile(ex);
                }
            }
            return debitCreated;
        }

        private bool CreatedDebitForSample(long sdgId, bool debug, string SdgIdSring, string sampleIdSring, long sampleId,
                                           bool debitCreated)
        {
            //find "part" connected to SDG by   SDG->U_ORDER(s)->COSTUMER->PRICE_LIST_name->PART
            SDG sdg = dal.FindBy<SDG>(d => d.SDG_ID == sdgId).SingleOrDefault();
            if ("XR".Contains(sdg.STATUS))
            {
                if (debug) MessageBox.Show("The Sample is in status X or R and cannnot be given a debit");
                return false;
            }
            //U_ORDER_USER[] orders = dal.FindBy<U_ORDER_USER>(ou => ou.U_SDG_NAME == sdg.NAME && ou.U_STATUS != "X").ToArray();
            U_ORDER_USER order;
            if (sdg.SDG_USER.U_ORDER == null)
            {
                if (debug) MessageBox.Show("Can't create debit. order not defined");
            }
            else
            {
                order = sdg.SDG_USER.U_ORDER.U_ORDER_USER;
                //    foreach (U_ORDER_USER order in orders)

                {
                    U_CUSTOMER_USER customer =
                        dal.FindBy<U_CUSTOMER_USER>(cu => cu.U_CUSTOMER_ID == order.U_CUSTOMER).SingleOrDefault();
                    //=order.U_CUSTOMER1.U_CUSTOMER_USER;// 
                    if (customer == null)
                    {
                        if (debug) MessageBox.Show("Can't create debit. Customer not defined");
                    }
                    else if (customer.U_PRICE_LIST_NAME == null)
                    {
                        if (debug)
                            MessageBox.Show("Can't create debit. Price List not defined for Customer " +
                                            customer.U_CUSTOMER.NAME);
                    }
                    else
                    {
                        //		sample->SDG ->	Order			קיים
                        //     Order->Customer        קיים
                        //     Customer->Customer_user       קיים
                        //      Customer_user->u_ u_price_list_name Link    (צריך לבדוק, צריך להיות קיים)

                        //i.עבור    Customer    ועבור   u_order_user.u_part_Code
                        //ii.אם קיימת רשומת מחיר  במחירון(לפי שם מחירון, ופריט ההזמנה)
                        //iii.עם  u_price_list_name and u_part_Code
                        // ןלא קיים   entityname=sample name
                        //iv.הכנס מחיר מהרשומה(יכול להיות – 0 או ריק שגם הוא - 0)
                        //v.אחרת – הכנס מחיר(-1) !
                        //f.בכל מקרה הוסף רישום ל – Sdg Log
                        //g.אחר כך המשך לבדוק רשומות אחרות מאותו מחירון u_price_list_name
                        //i.פריטים מאותו סוג    u_Parts_user.u_part_type    כמו סוג פריט ההזמנה
                        //ii.או מסוג Q כללי
                        //iii.רק פריטים שהשאילתא שלהם מחזירה ערך 1        הוסף ל - Debit
                        //iv.כל השאילתות מוגדרות עם פרמטר    Order_id    בלבד.


                        long sessionIdlong = (long)sessionId;

                        U_PARTS_USER[] parts = null;

                        U_PRICE_LIST_USER[] defaultPriceList = null;
                        if (order.U_PARTS_ID != null)
                        {
                            try
                            {
                                //for sample  plu.U_PARTS.U_PARTS_USER.U_PART_TYPE == "S"
                                defaultPriceList = dal
                                    .FindBy<U_PRICE_LIST_USER>(
                                        plu => plu.U_PRICE_LIST_NAME.Trim() == customer.U_PRICE_LIST_NAME.Trim()
                                               && plu.U_EFCT_FROM_DATE != null
                                               //&& plu.U_PART_ID == order.U_PARTS_ID
                                               && plu.U_EFCT_FROM_DATE < DateTime.Today
                                            && plu.U_PARTS.U_PARTS_USER.U_PART_TYPE == "S"
                                    //deal with this with a debug message
                                    //    && plu.U_PARTS.U_PARTS_USER.U_DISPLAY_ON_PRICE_LIST == "T"
                                    //   && plu.U_PARTS.U_PARTS_USER.U_QUERY != null
                                    )
                                    .OrderByDescending(plu => plu.U_EFCT_FROM_DATE).ToArray();
                            }
                            catch (Exception ex)
                            {
                                Logger.WriteLogFile(ex);
                                defaultPriceList = null;
                            }
                        }

                        if (defaultPriceList != null && defaultPriceList.Count() != 0)
                        {
                            if (debug)
                                MessageBox.Show("Calculating debit for price list \"" + customer.U_PRICE_LIST_NAME + "\"");

                            //U_PARTS_USER defaultPart = order.U_PARTS.U_PARTS_USER;
                            //dal.InsertToSdgLog(sdgId, "SDG.FirstDebitLine", sessionIdlong, order.U_ORDER.NAME + "," + customer.U_CUSTOMER.NAME
                            //                                                        + "," + customer.U_PRICE_LIST_NAME);
                            //DealWithSamplePrice(customer, defaultPart, order, sdg, SdgIdSring, ref debitCreated, debug);
                            ////parts = dal.FindBy<U_PARTS_USER>(pu => pu.U_DISPLAY_ON_PRICE_LIST == "T"
                            ////                                && (pu.U_PART_TYPE == defaultPart.U_PART_TYPE
                            ////                                    || pu.U_PART_TYPE == "Q")
                            ////                                && pu.U_QUERY != null).ToArray();

                            ////if there is a default price list for custoer, get its price 

                            int i = 0;
                            parts = new U_PARTS_USER[defaultPriceList.Count()];
                            foreach (U_PRICE_LIST_USER uPriceListUser in defaultPriceList)
                            {
                                parts[i++] = uPriceListUser.U_PARTS.U_PARTS_USER;
                            }
                        }
                        //if (parts.Count()==0 || defaultPriceList.Count()==0 || order.U_PARTS_ID == null)
                        // {
                        //     //SDG_LOG test = dal.FindBy<SDG_LOG>(dl => dl.SDG_ID == 611).FirstOrDefault();
                        //     dal.InsertToSdgLog(sdgId, "SDG.FirstDebitLine", sessionIdlong, order.U_ORDER.NAME + "," + customer.U_CUSTOMER.NAME
                        //                                                                + "," + ""
                        //                                                                + "," + "");
                        //     //if there is no default price list for custoer, get only price from Q group
                        //     if (debug)
                        //     {
                        //         if (defaultPriceList == null)
                        //         {
                        //             MessageBox.Show("There is no default price list for customer " + customer.U_CUSTOMER.NAME + ", get only price from S group");
                        //         }
                        //         if (order.U_PARTS == null)
                        //         {
                        //             MessageBox.Show("There is no default part for Order " + order.U_ORDER.NAME + ", get only price from S group");
                        //         }

                        //     }
                        //     parts = dal.FindBy<U_PARTS_USER>(pu => pu.U_DISPLAY_ON_PRICE_LIST == "T"
                        //                                      && pu.U_PART_TYPE == "S"
                        //                                      && pu.U_QUERY != null).ToArray();
                        // }
                        if (parts == null)
                        {
                            if (debug) MessageBox.Show("Can't create debit for sample. PARTS Missing");
                            //return;
                        }
                        else
                            foreach (U_PARTS_USER part in parts)
                            {
                                object value;
                                // get query in U_QUERY

                                string query = part.U_QUERY;
                                if (string.IsNullOrEmpty(query))
                                {
                                    value = null;
                                    if (debug)
                                        MessageBox.Show("Query for PART #" + part.U_PARTS.NAME + " is empty\r\n");
                                }
                                else if (part.U_DISPLAY_ON_PRICE_LIST != "T")
                                {
                                    if (debug)
                                        MessageBox.Show("PART #" + part.U_PARTS.NAME +
                                                        " is not calculated, U_DISPLAY_ON_PRICE_LIST is false\r\n");
                                    value = null;
                                }
                                else
                                {
                                    // replace #SDG_ID# and other fielsd in U_QUERY
                                    query = MyReplace(query, "#SDG_ID#", SdgIdSring);
                                    query = MyReplace(query, "#SAMPLE_ID#", sampleIdSring);
                                    query = MyReplace(query, "#ORDER_ID#", order.U_ORDER_ID.ToString());
                                    query = MyReplace(query, "#CUSTOMER_ID#", customer.U_CUSTOMER_ID.ToString());
                                    query = MyReplace(query, "#PRICE_LEVEL#", customer.U_PRICE_LEVEL ?? "");
                                    query = MyReplace(query, "#PART_CODE#", order.U_PARTS.U_PARTS_USER.U_PART_CODE ?? "");
                                    query = MyReplace(query, "\r", " ");
                                    query = MyReplace(query, "\n", " ");

                                    OracleCommand cmd = new OracleCommand(query, _connection);

                                    try
                                    {
                                        value = cmd.ExecuteScalar();


                                    }
                                    catch (Exception ex)
                                    {
                                        if (debug)
                                            MessageBox.Show("Error running query for PART #" + part.U_PARTS.NAME +
                                                            "\r\n" + ex);
                                        value = null;
                                        //continue loop 
                                    }
                                    finally
                                    {
                                        cmd.Dispose();
                                    }
                                    // Run query in U_QUERY, 
                                }
                                if (value == null)
                                {
                                    if (debug)
                                        MessageBox.Show("Can't create debit. PARTS query for PART #" + part.U_PARTS.NAME +
                                                        " returned null");
                                    //return;
                                }
                                else
                                {
                                    try
                                    {
                                        decimal? quantity = decimal.Parse(value.ToString());

                                        //If Qery returns true, get price
                                        SAMPLE sample =
                                            dal.FindBy<SAMPLE>(s => s.SAMPLE_ID == sampleId).SingleOrDefault();

                                        DealWithSamplePrice(customer, part, quantity, order, sample, SdgIdSring,
                                                            ref debitCreated, debug);
                                    }
                                    catch (Exception ex)
                                    {

                                        if (debug)
                                            MessageBox.Show("Error Parsing value to quantity for part :'" + part.U_PARTS.NAME +
                                                            "'");
                                        Logger.WriteLogFile(ex);

                                    }
                                    //}
                                }
                            }
                    }
                }
            }
            return debitCreated;
        }

        private void DealWithSamplePrice(U_CUSTOMER_USER customer, U_PARTS_USER part, decimal? quantity, U_ORDER_USER order, SAMPLE sample, string SdgIdSring, ref bool debitCreated, bool debug)
        {
            decimal? nullablePrice = dal.Run_GET_PRICE(customer.U_CUSTOMER_ID, part.U_PARTS_ID);

            if (nullablePrice == null)
            {
                if (debug) MessageBox.Show("Can't create debit for part " + part.U_PARTS.NAME + ". Price List query returned null");
                //ziv change in 22.3.16 email
                nullablePrice = -1;
                //change from 13/09/16 
                //don't create debit for getprice = null
                return;

            }
            // else
            {
                decimal price = (decimal)nullablePrice;
                decimal priceIncludingVAT;
                PHRASE_HEADER Params = dal.GetPhraseByName("System Parameters");
                string vatString;
                Params.PhraseEntriesDictonary.TryGetValue("Vat Precent", out vatString);
                vatString = vatString.Replace("%", "");
                decimal vat = Convert.ToDecimal(vatString);
                priceIncludingVAT = Decimal.Round(price * (vat / 100 + 1), 3);
                //if (customer.U_INC_VAT == "T")
                //{
                //    try
                //    {

                //        priceIncludingVAT = price;
                //        price = price / (vat / 100 + 1);
                //    }
                //    catch
                //    {
                //        if (debug) MessageBox.Show(@"Error, Debit not created. Could not find entry ""VAT"" in Phrase ""System Parameters""");
                //        return;
                //    }
                //}
                //else
                //{
                //    priceIncludingVAT = price * (vat / 100 + 1);
                //}
                U_DEBIT_USER debit =
                    dal.FindBy<U_DEBIT_USER>(
                        du => du.U_SDG_NAME == sample.SDG.NAME
                             && du.U_ENTITY_ID == sample.NAME
                            && du.U_PARTS_ID == part.U_PARTS_ID
                            && du.U_DEBIT_STATUS != "X")
                       .SingleOrDefault();

                if (debit != null)
                {

                    if (debug) MessageBox.Show("Can't create debit for part " + part.U_PARTS.NAME + ". Debit already exists");
                    // return;

                }
                else
                {
                    string debitText = "צנצנת " + sample.SDG.SDG_USER.U_PATHOLAB_NUMBER + sample.NAME.Substring(10) +
                                       "  : " + part.U_PARTS.DESCRIPTION;
                    InsertDebit(ref debitCreated, sample.NAME, debitText, sample.SDG, order, part, quantity, price, priceIncludingVAT, "SAMPLE");

                }

            }
        }


        private void DealWithPrice(U_CUSTOMER_USER customer, U_PARTS_USER part, decimal? quantity, U_ORDER_USER order, SDG sdg, string SdgIdSring, ref bool debitCreated, bool debug)
        {
            decimal? nullablePrice = dal.Run_GET_PRICE(customer.U_CUSTOMER_ID, part.U_PARTS_ID);

            if (nullablePrice == null)
            {
                if (debug) MessageBox.Show("Can't create debit for part " + part.U_PARTS.NAME + ". Price List query returned null");
                //ziv change in 22.3.16 email
                nullablePrice = -1;
                //change from 13/09/16 
                //don't create debit for getprice = null
                return;

            }
            // else
            {
                decimal price = (decimal)nullablePrice;
                decimal priceIncludingVAT;
                PHRASE_HEADER Params = dal.GetPhraseByName("System Parameters");
                string vatString;
                Params.PhraseEntriesDictonary.TryGetValue("Vat Precent", out vatString);
                vatString = vatString.Replace("%", "");
                decimal vat = Convert.ToDecimal(vatString);
                priceIncludingVAT = Round3(price * (vat / 100 + 1));
                //if (customer.U_INC_VAT == "T")
                //{
                //    try
                //    {

                //        priceIncludingVAT = price;
                //        price = price * (vat / 100 + 1);
                //    }
                //    catch
                //    {
                //        if (debug) MessageBox.Show(@"Error, Debit not created. Could not find entry ""VAT"" in Phrase ""System Parameters""");
                //        return;
                //    }
                //}
                //else
                //{
                //    priceIncludingVAT = price * (vat / 100 + 1);
                //}
                U_DEBIT_USER debit =
                    dal.FindBy<U_DEBIT_USER>(
                        du => du.U_SDG_NAME == sdg.NAME
                            && du.U_PARTS_ID == part.U_PARTS_ID
                            && du.U_DEBIT_STATUS != "X")
                       .SingleOrDefault();



                if (debit != null)
                {

                    if (debug) MessageBox.Show("Can't create debit for part " + part.U_PARTS.NAME + ". Debit already exists");
                    // return;

                }
                else
                {
                    InsertDebit(ref debitCreated, sdg.NAME, part.U_PARTS.NAME, sdg, order, part, quantity, price, priceIncludingVAT, "SDG");

                }

            }
        }


        private long InsertDebit(ref bool debitCreated, string ObjectName, string partText, SDG sdg, U_ORDER_USER order, U_PARTS_USER part, decimal? quantity, decimal price, decimal priceIncludingVAT, string entityType, string descripton = "", DateTime? lastUpdate = null)
        {
            long sequenceId = (long)dal.GetNewId("SQ_U_DEBIT");

            if (lastUpdate == null) lastUpdate = dal.GetSysdate();
            U_DEBIT debit = new U_DEBIT
            {
                U_DEBIT_ID = (long)sequenceId,
                NAME = sequenceId.ToString(),
                DESCRIPTION = descripton,
                VERSION = "1",
                VERSION_STATUS = "A"
            };
            debit.U_DEBIT_USER = new U_DEBIT_USER
            {
                U_DEBIT_ID = (long)sequenceId,
                U_ORDER_ID = order.U_ORDER_ID,
                U_SDG_NAME = sdg.NAME,
                U_EVENT_DATE = dal.GetSysdate(),
                U_PARTS_ID = part.U_PARTS_ID,
                U_PART_TEXT = partText,
                U_PART_PRICE = Round3(price),
                U_PRICE_INC_VAT = Round3(priceIncludingVAT),
                U_QUANTITY = quantity,
                U_LINE_AMOUNT = Round3(priceIncludingVAT * quantity),
                U_ENTITY_ID = ObjectName,
                U_DEBIT_STATUS = "N",
                U_LAST_UPDATE = lastUpdate,
            };
            dal.Add(debit);
            dal.SaveChanges();
            dal.InsertToSdgLog(sdg.SDG_ID, entityType + ".Debit", (long)sessionId, ObjectName
                                                                                + "," + part.U_PARTS.NAME
                                                                                + "," + priceIncludingVAT);
            debitCreated = true;
            return sequenceId;


        }

        private decimal Round3(decimal? x)
        {

            return decimal.Round(x ?? 0, 3);
        }

        private string MyReplace(string input, string searchString, string repalaceWith)
        {
            return Regex.Replace(input, searchString, repalaceWith,
                                                        RegexOptions.IgnoreCase);

        }

        /// <summary>
        /// Init nautilus con
        /// </summary>
        /// <param name="ntlsCon"></param>
        /// <returns></returns>
        public OracleConnection GetConnection(INautilusDBConnection ntlsCon)
        {

            OracleConnection connection = null;

            if (ntlsCon != null)
            {


                // Initialize variables
                String roleCommand;
                // Try/Catch block
                try
                {
                    _connectionString = ntlsCon.GetADOConnectionString();

                    var splited = _connectionString.Split(';');

                    var cs = "";

                    for (int i = 1; i < splited.Count(); i++)
                    {
                        cs += splited[i] + ';';
                    }

                    var username = ntlsCon.GetUsername();
                    if (string.IsNullOrEmpty(username))
                    {
                        var serverDetails = ntlsCon.GetServerDetails();
                        cs = "User Id=/;Data Source=" + serverDetails + ";";
                    }

                    //Create the connection
                    connection = new OracleConnection(cs);

                    // Open the connection
                    connection.Open();

                    // Get lims user password
                    string limsUserPassword = ntlsCon.GetLimsUserPwd();

                    // Set role lims user
                    if (limsUserPassword == "")
                    {
                        // LIMS_USER is not password protected
                        roleCommand = "set role lims_user";
                    }
                    else
                    {
                        // LIMS_USER is password protected.
                        roleCommand = "set role lims_user identified by " + limsUserPassword;
                    }

                    // set the Oracle user for this connecition
                    OracleCommand command = new OracleCommand(roleCommand, connection);

                    // Try/Catch block
                    try
                    {
                        // Execute the command
                        command.ExecuteNonQuery();
                    }
                    catch (Exception f)
                    {
                        // Throw the exception
                        throw new Exception("Inconsistent role Security : " + f.Message);
                    }

                    // Get the session id
                    sessionId = ntlsCon.GetSessionId();

                    // Connect to the same session
                    string sSql = string.Format("call lims.lims_env.connect_same_session({0})", sessionId);

                    // Build the command
                    command = new OracleCommand(sSql, connection);

                    // Execute the command
                    command.ExecuteNonQuery();

                }
                catch (Exception e)
                {
                    // Throw the exception
                    throw e;
                }
                // Return the connection
            }
            return connection;
        }


      
    }
}
