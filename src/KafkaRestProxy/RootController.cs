﻿using Microsoft.AspNetCore.Mvc;

namespace pro.savel.KafkaRestProxy
{
    [ApiController]
    [Route("")]
    public class RootController : ControllerBase
    {
        [HttpGet]
        public string GetVersion()
        {
            return "0.0.1";
        }
    }
}