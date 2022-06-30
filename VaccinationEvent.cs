using System;

namespace vac_seen_rollup
{
    class VaccinationEvent
    {
        public String? id;
        public String? RecipientID;
        public DateTime EventTimestamp;
        public string? CountryCode;
        public String? VaccinationType;
        public int ShotNumber;
    }
}
