// ============================================
//  Script: facebook_queries.js
//  Autor: juan david mora daza
//  Descripción: Consultas y análisis del dataset
//               Facebook Metrics en MongoDB
// ============================================

// Seleccionar la base de datos
const databaseName = "facebookMetricsDB";
db = db.getSiblingDB(databaseName);

// ============================================
// 1️ Verificación de datos cargados
// ============================================

print("\n--- Primeros 5 documentos cargados ---\n");
db.posts.find().limit(5).forEach(printjson);

// ============================================
// 2️ Consultas básicas (CRUD)
// ============================================

// INSERTAR un documento de prueba
print("\n--- Insertar nuevo documento ---\n");
db.posts.insertOne({
  Type: "Video",
  Category: 3,
  Post_Month: 8,
  Post_Weekday: 5,
  Post_Hour: 18,
  Paid: 0,
  Lifetime_Post_Total_Reach: 28000,
  Lifetime_Engaged_Users: 980,
  Like: 340,
  Comment: 45,
  Share: 28,
  Total_Interactions: 413
});

// SELECCIONAR documentos tipo "Video"
print("\n--- Seleccionar publicaciones tipo 'Video' ---\n");
db.posts.find({ Type: "Video" }).limit(5).forEach(printjson);

// ACTUALIZAR publicaciones tipo Photo -> Paid = 1
print("\n--- Actualizar publicaciones tipo 'Photo' ---\n");
db.posts.updateMany(
  { Type: "Photo" },
  { $set: { Paid: 1 } }
);

// ELIMINAR publicaciones con menos de 10 interacciones
print("\n--- Eliminar publicaciones con menos de 10 interacciones ---\n");
db.posts.deleteMany({ Total_Interactions: { $lt: 10 } });

// ============================================
// 3️ Consultas con filtros y operadores
// ============================================

print("\n--- Publicaciones de tipo Video con más de 1000 likes ---\n");
db.posts.find({
  Type: "Video",
  Like: { $gt: 1000 }
}).forEach(printjson);

print("\n--- Publicaciones hechas fin de semana (sábado y domingo) ---\n");
db.posts.find({
  Post_Weekday: { $in: [6, 7] }
}).forEach(printjson);

print("\n--- Publicaciones no pagadas con más de 500 interacciones ---\n");
db.posts.find({
  Paid: 0,
  Total_Interactions: { $gt: 500 }
}).forEach(printjson);

// ============================================
// 4️ Consultas de agregación (estadísticas)
// ============================================

// Conteo por tipo de publicación
print("\n--- Conteo de publicaciones por tipo ---\n");
db.posts.aggregate([
  { $group: { _id: "$Type", count: { $sum: 1 } } }
]).forEach(printjson);

// Promedio de alcance por tipo de publicación
print("\n--- Promedio de alcance total por tipo ---\n");
db.posts.aggregate([
  { $group: { _id: "$Type", avgReach: { $avg: "$Lifetime_Post_Total_Reach" } } },
  { $sort: { avgReach: -1 } }
]).forEach(printjson);

// Total de interacciones agrupadas por categoría
print("\n--- Total de interacciones por categoría ---\n");
db.posts.aggregate([
  { $group: { _id: "$Category", totalInteractions: { $sum: "$Total_Interactions" } } },
  { $sort: { _id: 1 } }
]).forEach(printjson);

// Promedio de likes, comentarios y shares por día de la semana
print("\n--- Promedios de interacción por día de la semana ---\n");
db.posts.aggregate([
  {
    $group: {
      _id: "$Post_Weekday",
      avgLikes: { $avg: "$Like" },
      avgComments: { $avg: "$Comment" },
      avgShares: { $avg: "$Share" }
    }
  },
  { $sort: { _id: 1 } }
]).forEach(printjson);

// Top 5 publicaciones con mayor número de interacciones
print("\n--- Top 5 publicaciones con más interacciones ---\n");
db.posts.find().sort({ Total_Interactions: -1 }).limit(5).forEach(printjson);

// ============================================
// 5️ Fin del script
// ============================================

print("\n--- Script finalizado correctamente ---\n");