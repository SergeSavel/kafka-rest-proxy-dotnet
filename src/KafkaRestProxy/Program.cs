// Copyright 2024 Sergey Savelev
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Reflection;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting.Systemd;
using Microsoft.Extensions.Hosting.WindowsServices;
using Microsoft.OpenApi.Models;
using SergeSavel.KafkaRestProxy.AdminClient;
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.Common.Exceptions;
using SergeSavel.KafkaRestProxy.Common.Extensions;
using SergeSavel.KafkaRestProxy.Consumer;
using SergeSavel.KafkaRestProxy.Producer;
using SergeSavel.KafkaRestProxy.Proxy;
using SergeSavel.KafkaRestProxy.SchemaRegistry;

var webAppOptions = new WebApplicationOptions
{
    Args = args,
    ContentRootPath = WindowsServiceHelpers.IsWindowsService() || SystemdHelpers.IsSystemdService()
        ? AppContext.BaseDirectory
        : default
};

var builder = WebApplication.CreateBuilder(webAppOptions);

builder.Host.UseWindowsService();
builder.Host.UseSystemd();

builder.Services.Configure<IISServerOptions>(options => { options.MaxRequestBodySize = int.MaxValue; });
builder.Services.Configure<KestrelServerOptions>(options => { options.Limits.MaxRequestBodySize = int.MaxValue; });

// Add services to the container.

builder.Services.AddControllers(options => options.Filters.Add(new HttpResponseExceptionFilter()))
    .AddJsonOptions(options =>
    {
        options.JsonSerializerOptions.WriteIndented = false;
        //options.JsonSerializerOptions.IgnoreNullValues = true;
        options.JsonSerializerOptions.PropertyNamingPolicy = null;
        options.JsonSerializerOptions.Converters.Add(
            new JsonStringEnumConverter(new JsonStraightNamingPolicy()));
    });

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v5", new OpenApiInfo { Title = "KafkaRestProxy", Version = "v5" });
    var xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
    var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
    options.IncludeXmlComments(xmlPath);
});

builder.Services.AddHealthChecks();
builder.Services.AddCommon(builder.Configuration);
builder.Services.AddProxy(builder.Configuration);
builder.Services.AddAdminClient(builder.Configuration);
builder.Services.AddConsumer(builder.Configuration);
builder.Services.AddProducer(builder.Configuration);
builder.Services.AddSchemaRegistry(builder.Configuration);

var app = builder.Build();

// Configure the HTTP request pipeline.

if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
    app.UseSwagger();
    app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v5/swagger.json", "KafkaRestProxy v5"));
}

app.UseRouting();
app.UseAuthentication();
app.UseAuthorization();

app.MapHealthChecks("/health");
app.MapControllers();

app.Run();