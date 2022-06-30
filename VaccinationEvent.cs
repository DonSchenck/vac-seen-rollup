using System;

namespace vac_seen_rollup
{
    public class VaccinationEvent
    {
        public String? id;
        public String? RecipientID;
        public DateTimeOffset EventTimestamp;
        public string? CountryCode;
        public String? VaccinationType;
        public int ShotNumber;
    }
}
