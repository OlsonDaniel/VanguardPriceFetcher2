using Newtonsoft.Json.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Net.Mail;
using System.Reflection;

namespace VanguardPriceFetcher2
{

    internal class Program
    {

        private static void Main(string[] args)
        {
            try
            {
                string message = "";

                List<VanguardPrices> vanguardPrices = GetVanguardPrices();
                message += PriceUpload(vanguardPrices);

                string to = "b305441f.JasperRidgePartners.onmicrosoft.com@amer.teams.ms";
                SendMail("no-reply@jasperridge.com", to, "Vanguard Price Upload", message);
                to = "dolson@jasperridge.com";
                SendMail("no-reply@jasperridge.com", to, "Vanguard Prices", message);
            }
            catch (Exception ex)
            {
                SendErrorAlert(MethodBase.GetCurrentMethod().Name, ex);
            }
        }

        private static string PriceUpload(List<VanguardPrices> vgPrices)
        {
            try
            {
                string entityName = "VTR25";
                string connStr = "Data Source=SQLCLUSTER;Initial Catalog=JRP7012PROD;Integrated Security=True";

                System.Text.StringBuilder sb = new();
                int blankRows = 0;

                using (SqlConnection conn = new(connStr))
                {
                    conn.Open();
                    using SqlCommand cmd = new("CS_spMktDataImport", conn);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@EntityName", SqlDbType.VarChar, 50);
                    cmd.Parameters.Add("@EntityType", SqlDbType.VarChar, 50);
                    cmd.Parameters.Add("@ProviderName", SqlDbType.VarChar, 50);
                    cmd.Parameters.Add("@PriceVariant", SqlDbType.VarChar, 50);
                    cmd.Parameters.Add(new SqlParameter("@Price", SqlDbType.Decimal)
                    {
                        Precision = 28,
                        Scale = 15
                    });
                    cmd.Parameters.Add("@PriceDate", SqlDbType.DateTime);
                    cmd.Parameters.Add("@ChangedBy", SqlDbType.VarChar, 128);

                    cmd.Parameters.Add("@ErrorCode", SqlDbType.Int).Direction = ParameterDirection.Output;
                    cmd.Parameters.Add("@ErrorDescription", SqlDbType.VarChar, 100).Direction = ParameterDirection.Output;

                    //first row name of deal
                    //second row header
                    foreach (VanguardPrices vgPrice in vgPrices)
                    {
                        DateTime endDate = vgPrice.EffectiveDate;
                        Console.WriteLine($"{endDate.ToShortDateString()} {vgPrice.Price}");
                        if (endDate.CompareTo(DateTime.MinValue) == 0)
                        {
                            blankRows++;
                        } else
                        {
                            //process row
                            cmd.Parameters["@EntityName"].Value = entityName;
                            cmd.Parameters["@EntityType"].Value = "Equity";
                            cmd.Parameters["@ProviderName"].Value = null;
                            cmd.Parameters["@PriceVariant"].Value = "Closing";
                            cmd.Parameters["@Price"].Value = vgPrice.Price;
                            cmd.Parameters["@PriceDate"].Value = endDate;
                            cmd.Parameters["@ChangedBy"].Value = null;

                            cmd.ExecuteNonQuery();

                            int errorCode = MakeInt(cmd.Parameters["@ErrorCode"].Value);
                            if (errorCode > 0)
                            {
                                string message = $"{errorCode} - {cmd.Parameters["@ErrorDescription"].Value}";
                                Console.WriteLine(message);
                                SendErrorAlert(MethodBase.GetCurrentMethod().Name, new Exception(message));
                            }
                            sb.AppendLine($"{endDate.ToShortDateString()} {vgPrice.Price}");
                        }
                        if (blankRows > 3) break;
                    }
                    conn.Close();
                }
                return sb.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                SendErrorAlert(MethodBase.GetCurrentMethod().Name, ex);
                return ex.Message;
            }
        }

        private static List<VanguardPrices> GetVanguardPrices()
        {
            //string json0  = /*lang=json,strict*/ @"[{""fundPrices"":{""metadata"":{""pagination"":{""next"":false,""offset"":0,""limit"":100,""count"":2,""totalCount"":2,""totalPages"":1}},""content"":[{""portId"":""1475"",""currencyCode"":""USD"",""timePeriodCode"":""D"",""priceTypeCode"":""NAV"",""effectiveDate"":""2024-02-23"",""price"":45.93},{""portId"":""1475"",""currencyCode"":""USD"",""timePeriodCode"":""D"",""priceTypeCode"":""NAV"",""effectiveDate"":""2024-02-26"",""price"":45.83}]},""fiftyTwoWeekHighLow"":{""metadata"":{""pagination"":{""next"":false,""offset"":0,""limit"":100,""count"":1,""totalCount"":1,""totalPages"":1}},""content"":[{""portId"":""1475"",""effectiveDate"":""2024-02-27"",""priceTypeCode"":""NAV"",""currencyCode"":""USD"",""highPrice"":45.93,""highDate"":""2024-02-23"",""lowPrice"":40.11,""lowDate"":""2023-03-10"",""timePeriod"":""52 Week""}]},""premiumDiscountDaily"":{""metadata"":{""pagination"":{""next"":false,""offset"":0,""limit"":100,""count"":0,""totalCount"":0,""totalPages"":0},""unrepresentedIdentifiers"":[""1475""]}}}]";
            DateTime endDate = DateTime.Today.AddDays(-1);
            DateTime startDate = DateTimeExtensions.GetStartDate(endDate);
            string startEndDates = $"{startDate:yyyy-MM-dd}:to:{endDate:yyyy-MM-dd}";
            string url = $"https://institutional.vanguard.com/investments/valuationPricesServiceProxy"
                         + $"?timePeriodCode=D&portIds=1475&priceTypeCodes=MKTP,NAV,HIGH,LOW,SNAV&effectiveDate={startEndDates}&offset=0";
            using HttpClient client = new();
            Task<string> response = client.GetStringAsync(url);
            string json = /*lang=json,strict*/ $"[{response.Result}]";
            dynamic vgD = JArray.Parse(json);
            dynamic content = vgD[0].fundPrices.content;

            List<VanguardPrices> vgPrices = [];
            foreach (var c in content)
            {
                vgPrices.Add(new VanguardPrices()
                {
                    EffectiveDate = (DateTime)c.effectiveDate,
                    Price = (decimal)c.price
                });
            }

            return vgPrices;
        }

        public static int MakeInt(object item)
        {
            if (item != null)
            {
                try
                {
                    return Convert.ToInt32(item);
                }
                catch
                {
                    return 0;
                }
            } else
            {
                return 0;
            }
        }

        public static DateTime MakeDateTime(object item)
        {
            if (item != null)
            {
                try
                {
                    return Convert.ToDateTime(item);
                }
                catch
                {
                    return DateTime.MinValue;
                }
            } else
            {
                return DateTime.MinValue;
            }
        }

        public static decimal MakeDecimal(object item)
        {
            if (item != null)
            {
                try
                {
                    return Convert.ToDecimal(item);
                }
                catch
                {
                    return 0;
                }
            } else
            {
                return 0;
            }
        }

        public static void SendErrorAlert(string function, Exception ex)
        {
            string to = "dolson@jasperridge.com";
            string from = to;
            string message = $"{function} : {Environment.UserName}{Environment.NewLine}{ex}";

            try
            {
                SendMail(from, to, "Alert! Vanguard Price Fetcher Error", message);
            }
            catch { }
        }

        public static string SendMail(string from, string to, string subject, string body)
        {
            try
            {
                string host = "fwrelay.jasperridge.com";
                SmtpClient client = new(host, 25)
                {
                    DeliveryMethod = SmtpDeliveryMethod.Network
                };
                client.Send(new MailMessage(from, to, subject, body));
                return "Mail Sent";
            }
            catch (Exception ex)
            {
                return ex.ToString();
            }
        }
    }

    public class VanguardPrices
    {
        public DateTime EffectiveDate { get; set; }
        public decimal Price { get; set; }
    }

    public static class DateTimeExtensions
    {
        public static DateTime GetStartDate(DateTime endDate)
        {
            DateTime startDate = endDate.AddDays(-3);
            switch (endDate.DayOfWeek)
            {
                case DayOfWeek.Sunday:
                    startDate = endDate.AddDays(-5);
                    break;
                case DayOfWeek.Monday:
                    startDate = endDate.AddDays(-5);
                    break;
                case DayOfWeek.Tuesday:
                    startDate = endDate.AddDays(-5);
                    break;
                case DayOfWeek.Wednesday:
                    startDate = endDate.AddDays(-6);
                    break;
                case DayOfWeek.Thursday:
                    startDate = endDate.AddDays(-6);
                    break;
                case DayOfWeek.Friday:
                    startDate = endDate.AddDays(-4);
                    break;
                case DayOfWeek.Saturday:
                    startDate = endDate.AddDays(-4);
                    break;
            }

            return startDate;
        }
    }
}