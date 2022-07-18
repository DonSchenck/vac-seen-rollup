﻿using System;

namespace vac_seen_rollup
{
    public class VaccinationEvent
    {
        public Guid Id;
        public String RecipientID;
        public DateTime EventTimestamp;
        public string CountryCode;
        public String VaccinationType;
        public int ShotNumber;
    }
}
