using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CalculateDebit;
using Oracle.ManagedDataAccess.Client;
using Patholab_Common;
using Patholab_DAL_V1;

namespace Run_Calculate_Debit
{
    class Program
    {
        static private CalculateDebitCls calculateDebitCls = new CalculateDebitCls();
        private static OracleConnection connection;
        static void Main(string[] args)
        {
            Log("Starting");


            DataLayer dal = new DataLayer();
            connection = new OracleConnection("Data Source=LIMSPROD;User ID=lims_sys;Password=lims_sys");
            try
            {
                bool debug = false;

                // calculateDebitCls = new CalculateDebitCls();
                calculateDebitCls.Init(dal, connection, debug);
                connection.Open();
                // Log(calculateDebitCls.SingleExecute());

                dal.MockConnect();

                //todo: connect with a connection string like a normal person
                Log("RUNNING ON SDGs:\r\n");
                var sdgs = dal.FindBy<SDG>(d => d.SDG_USER.U_CALCULATE_DEBIT == "T"
                                           && d.STATUS != "X"

                                            ).Include(x => x.SDG_USER)
                                 .Include(x => x.SDG_USER.U_ORDER)
                                 .Include(x => x.SDG_USER.U_ORDER.U_ORDER_USER)
                                 .Include(x => x.SDG_USER.IMPLEMENTING_PHYSICIAN)
                                 .Include(x => x.SDG_USER.IMPLEMENTING_PHYSICIAN.SUPPLIER_USER)
                                 .Include(x => x.SDG_USER.U_ORDER.U_ORDER_USER.U_CUSTOMER1)
                                 .OrderBy(x => x.SDG_ID);
                foreach (var sdg in sdgs)
                {
                    Log(sdg.NAME + ", ");

                    bool res = false;
                    Exception ex = null;
                    try
                    {
                        res = calculateDebitCls.RunOnSDG(sdg);
                    }
                    catch (Exception e)
                    {
                        res = false;
                        ex = e;
                    }
                    if (res)
                    {
                        sdg.SDG_USER.U_CALCULATE_DEBIT = "F";
                        dal.SaveChanges();

                    }
                    else
                    {
                        Log("<=Error, ");
                        if ("\r\n" + ex != null) Log(ex + "\r\n");

                    }
                }
                Log("\r\nRUNNING ON ALIQUOTS:\r\n");
                var aliquots = dal.FindBy<ALIQUOT>(a => a.ALIQUOT_USER.U_CALCULATE_DEBIT == "T" && a.STATUS != "X")
                    .Include(x => x.ALIQUOT_USER)
                    .Include(x => x.SAMPLE)
                    .Include(x => x.SAMPLE.SAMPLE_USER)
                    .Include(x => x.SAMPLE.SDG)
                    .Include(x => x.SAMPLE.SDG.SDG_USER)
                    .Include(x => x.SAMPLE.SDG.SDG_USER.U_ORDER)
                    .Include(x => x.SAMPLE.SDG.SDG_USER.U_ORDER.U_ORDER_USER)
                    .Include(x => x.SAMPLE.SDG.SDG_USER.IMPLEMENTING_PHYSICIAN)
                    .Include(x => x.SAMPLE.SDG.SDG_USER.IMPLEMENTING_PHYSICIAN.SUPPLIER_USER)
                    .Include(x => x.SAMPLE.SDG.SDG_USER.U_ORDER.U_ORDER_USER.U_CUSTOMER1)
                    .OrderBy(x => x.ALIQUOT_ID)
;
                foreach (var alquout in aliquots)
                {
                    Log(alquout.NAME);
                    bool res = false;
                    Exception ex = null;
                    try
                    {
                        res = calculateDebitCls.RunOnAliquot(alquout);
                    }
                    catch (Exception e)
                    {
                        res = false;
                        ex = e;
                    }
                    if (res)
                    {
                        alquout.ALIQUOT_USER.U_CALCULATE_DEBIT = "F";
                        dal.SaveChanges();
                        Log(",");
                    }
                    else
                    {
                        Log("<=Error, ");
                        if ("\r\n" + ex != null) Log(ex + "\r\n");
                    }
                }

            }
            catch (Exception ex)
            {
                // if (debug) MessageBox.Show("תקלה ביצירת רשומת חיוב");
                Log("\r\n" + ex + "\r\n");
                Logger.WriteLogFile(ex);
            }
            finally
            {
                Log("\r\nFinished!! Goodbye :)\r\n");
                if (dal != null) dal.Close();
                dal = null;
                if (connection != null)
                    connection.Close();
                connection = null;
            }

            Console.ReadLine();
        }
        static void Log(string log)
        {
            Console.WriteLine(log);

            Logger.WriteLogFile(log);
        }
    }
}
