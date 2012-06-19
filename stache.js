/*
 * TODO
 * Some parts of this class are highly inefficient and should be optimized later
 */
com.anvesaka.common.namespace("com.anvesaka.stache").Index = Class.extend({
	init:function(options) {
		var thiz = this;
		options = com.anvesaka.common.extend(true, {
			prop:"id",
			pk:"id",
			unique:true
		},
		options);
		this._private = options;
		this._private.bplusTree = new com.anvesaka.bplus.BPlusTree({
			order:100
		}); 
	},
	add:function(entity) {
		var key = entity[this._private.prop];
		if (com.anvesaka.common.isNotDefined(key)) {
			return false;
		}
		var tree = this._private.bplusTree;
		if (this._private.unique) {
			var existing = tree.insert(key, entity);
			if (entity[this._private.pk]!=existing[this._private.pk]) {
				return false;
			}
			else {
				return true;
			}
		}
		else {
			var list = tree.find(key);
			if (list) {
				list.push(entity);
			}
			else {
				list = [entity];
				tree.insert(key, list);
			}
			return true;
		}
	},
	remove:function(entity) {
		if (entity&&entity._stache) {
			var key = entity[this._private.prop];
			var id = entity[this._private.pk];
			var tree = this._private.bplusTree;
			if (this._private.unique) {
				return tree.remove(key)
			}
			else {
				var list = tree.find(key);
				var removed = undefined;
				if (list) {
					for  (var i=0; i<list.length; i++) {
						if (list[i][this._private.pk]===id) {
							removed = list.splice(i, 1)[0];
							break;
						}
					}
					if (list.length==0) {
						tree.remove(key);
					}
				}
				return removed;
			}
		}
		else {
			return this._private.bplusTree.remove(entity);
		}
	},
	get:function(keys) {
		var thiz = this;
		var retval = [];
		keys.forEach(function(key) {
			var value = thiz._private.bplusTree.find(key);
			if (value) {
				retval.push(value);
			}
		});
		return this._private.unique?retval:com.anvesaka.common.flatten(retval);
	},
	range:function(startKey, endKey) {
		return this._private.unique?this._private.bplusTree.range(startKey, endKey):com.anvesaka.common.flatten(this._private.bplusTree.range(startKey, endKey));
	}
});

com.anvesaka.common.namespace("com.anvesaka.stache").Table = Class.extend({
	init:function(options) {
		var thiz = this;
		options = com.anvesaka.common.extend(true, {
			pk:"id",
			schema:{
				id:{
					unique:true,
					index:true
				}
			},
			schemaDefaults:{
				index:false,
				unique:true,
				type:"number"
			}
		},
		options);
		com.anvesaka.common.assertQuack({
			name:com.anvesaka.common.STRING,
			db:com.anvesaka.common.OBJECT
		}, options);
		this._private = options;
	},
	processSchema:function() {
		var thiz = this;
		this._private.indexes = {};
		this._private.associations = {};
		this._private.schemaMethods = {
			getId:function() {
				return this[thiz._private.pk];
			}
		};
		com.anvesaka.common.keys(this._private.schema).forEach(function(prop) {
			thiz._private.schema[prop] = com.anvesaka.common.extend(true, {}, thiz._private.schemaDefaults, thiz._private.schema[prop]);
			var propertyDef = thiz._private.schema[prop];
			if (propertyDef.index) {
				thiz._private.indexes[prop] = new com.anvesaka.stache.Index({
					prop:prop,
					pk:thiz._private.pk,
					unique:com.anvesaka.common.isDefined(propertyDef.unique)?propertyDef.unique:true
				});
			}
			if (com.anvesaka.common.isDefined(propertyDef.association)) {
				thiz._private.associations[prop] = propertyDef.association;
			}
			var cappedPropName = com.anvesaka.common.capFirst(prop);
			var cappedInversePropName;
			if (propertyDef.inverse) {
				cappedInversePropName = com.anvesaka.common.capFirst(propertyDef.inverse)
			}
			if (propertyDef.type==="set"||propertyDef.type==="list") {
				if (propertyDef.association) {
					var associationTable = thiz._private.db.getTable(propertyDef.association);
					thiz._private.schemaMethods["get"+cappedPropName] = function(index) {
						var retval = [];
						var stale = [];
						this[prop].forEach(function(fk) {
							var value = associationTable.get(["ID", fk]);
							if (com.anvesaka.common.isDefined(value)) {
								retval.push(value);
							}
							else {
								stale.push(fk);
							}
						});
						if (stale.length>0) {
							this[prop] = com.anvesaka.common.difference([this[prop], stale])[0];
						}
						return retval;
					};
					thiz._private.schemaMethods["remove"+cappedPropName] = function(options) {
						var ids;
						var values;
						var thiz = this;
						if (options.values) {
							ids = options.values.map(function(value) {
								return value.getId();
							});
							values = options.values;
						}
						else if (options.ids) {
							ids = options.ids;
							var criteria = ["ID"];
							criteria = criteria.push.apply(criteria, ids);
							values = associationTable.get(criteria);
						}
						this[prop] = com.anvesaka.common.difference([this[prop], ids])[0];
						if (propertyDef.inverse) {
							values.forEach(function(value) {
								if (!value._stache.inverseToken) {
									thiz._stache.inverseToken = true;
									value["dissociate"+cappedInversePropName](thiz);
									delete thiz._stache.inverseToken;
								}
							});
						}
					};
					if (propertyDef.type==="set") {
						thiz._private.schemaMethods["add"+cappedPropName] = function(values) {
							var thiz = this;
							values = associationTable.mergeEntities(values);
							this[prop] = com.anvesaka.common.union([this[prop], values.map(function(value) {
								return value.getId();
							})]);
							if (propertyDef.inverse) {
								values.forEach(function(value) {
									if (!value._stache.inverseToken) {
										thiz._stache.inverseToken = true;
										value["associate"+cappedInversePropName](thiz);
										delete thiz._stache.inverseToken;
									}
								});
							}
						};
					}
					else {
						thiz._private.schemaMethods["add"+cappedPropName] = function(values) {
							var thiz = this;
							values = associationTable.mergeEntities(values);
							this[prop].push.apply(this[prop], values.map(function(value) {
								return value.getId();
							}));
							if (propertyDef.inverse) {
								values.forEach(function(value) {
									if (!value._stache.inverseToken) {
										thiz._stache.inverseToken = true;
										value["associate"+cappedInversePropName](thiz);
										delete thiz._stache.inverseToken;
									}
								});
							}
						};
					}
					thiz._private.schemaMethods["associate"+cappedPropName] = function(value) {
						this["add"+cappedPropName]([value]);
					};
					thiz._private.schemaMethods["dissociate"+cappedPropName] = function(value) {
						this["remove"+cappedPropName]({
							values:[value]
						});
					};
				}
				else {
					thiz._private.schemaMethods["get"+cappedPropName] = function(index) {
						return this[prop];
					};
					thiz._private.schemaMethods["remove"+cappedPropName] = function(values) {
						this[prop] = com.anvesaka.common.difference([this[prop], values])[0];
					};
					if (propertyDef.type==="set") {
						thiz._private.schemaMethods["add"+cappedPropName] = function(values) {
							this[prop] = com.anvesaka.common.union([this[prop], values]);
						};
					}
					else {
						thiz._private.schemaMethods["add"+cappedPropName] = function(values) {
							this[prop].push.apply(this[prop], values);
						};
					}
				}
				propertyDef.init = function(values) {
					this[prop] = [];
					if (values&&values.length>0) {
						this["add"+cappedPropName](values);
					}
				};
			}
			else if (propertyDef.type==="object") {
				if (propertyDef.association&&propertyDef.index) {
					var associationTable = thiz._private.db.getTable(propertyDef.association);
					thiz._private.schemaMethods["get"+cappedPropName] = function() {
						return associationTable.get(["ID", this[prop]])
					};
					thiz._private.schemaMethods["set"+cappedPropName] = function(value) {
						var existingValueId = this[prop];
						if (value) {
							value = associationTable.mergeEntity(value);
							if (value.getId()!=existingValueId) {
								this[prop] = value.getId();
								if (thiz._private.indexes[prop].add(this)) {
									thiz._private.indexes[prop].remove(existingValueId);
									if (propertyDef.inverse&&!value._stache.inverseToken) {
										this._stache.inverseToken = true;
										value["associate"+cappedInversePropName](this);
										delete this._stache.inverseToken;
									}
								}
								else {
									this[prop] = existingValueId;
									throw({
										message:"Cannot set value of "+thiz._private.name+":"+prop+" to "+value+" because this violates a unique constraint."
									});
								}
							}
						}
						else if (existingValueId) {
							thiz._private.indexes[prop].remove(this);
							this[prop] = undefined;
							if (propertyDef.inverse) {
								var existingValue = associationTable.get(["ID", existingValueId]);
								if (!existingValue._stache.inverseToken) {
									this._stache.inverseToken = true;
									existingValue["dissociate"+cappedInversePropName](this);
									delete this._stache.inverseToken;
								}
							}
						}
					};
					thiz._private.schemaMethods["associate"+cappedPropName] = function(value) {
						this["set"+cappedPropName](value);
					};
					thiz._private.schemaMethods["dissociate"+cappedPropName] = function(value) {
						this["set"+cappedPropName](undefined);
					};
				}
				else if (propertyDef.association) {
					var associationTable = thiz._private.db.getTable(propertyDef.association);
					thiz._private.schemaMethods["get"+cappedPropName] = function() {
						return associationTable.get(["ID", this[prop]])
					};
					thiz._private.schemaMethods["set"+cappedPropName] = function(value) {
						if (value) {
							value = associationTable.mergeEntity(value);
							this[prop] = value.getId();
							if (propertyDef.inverse&&!value._stache.inverseToken) {
								this._stache.inverseToken = true;
								value["associate"+cappedInversePropName](this);
								delete this._stache.inverseToken;
							}
						}
						else {
							this[prop] = undefined;
							if (propertyDef.inverse&&!value._stache.inverseToken) {
								this._stache.inverseToken = true;
								value["dissociate"+cappedInversePropName](this);
								delete this._stache.inverseToken;
							}
						}
					};
					thiz._private.schemaMethods["associate"+cappedPropName] = function(value) {
						this["set"+cappedPropName](value);
					};
					thiz._private.schemaMethods["dissociate"+cappedPropName] = function(value) {
						this["set"+cappedPropName](undefined);
					};
				}
				else if (propertyDef.index) {
					com.anvesaka.common.fail("Cannot define an index on an unassociated object property: "+thiz._private.name+":"+prop);
				}
				else {
					thiz._private.schemaMethods["get"+cappedPropName] = function() {
						return this[prop];
					};
					thiz._private.schemaMethods["set"+cappedPropName] = function(value) {
						this[prop] = value;
					};
				}
				propertyDef.init = function(value) {
					delete this[prop];
					if (value) {
						this["set"+cappedPropName](value);
					}
				};
			}
			else {
				if (propertyDef.index) {
					thiz._private.schemaMethods["get"+cappedPropName] = function() {
						return this[prop];
					};
					thiz._private.schemaMethods["set"+cappedPropName] = function(value) {
						var existingValueId = this[prop];
						if (com.anvesaka.common.isDefined(value)) {
							if (value!=existingValueId) {
								this[prop] = value;
								if (thiz._private.indexes[prop].add(this)) {
									thiz._private.indexes[prop].remove(existingValueId);
								}
								else {
									this[prop] = existingValueId;
									throw({
										message:"Cannot set value of "+thiz._private.name+":"+prop+" to "+value+" because this violates a unique constraint."
									});
								}
							}
						}
						else if (com.anvesaka.common.isDefined(existingValueId)) {
							thiz._private.indexes[prop].remove(this);
							this[prop] = undefined;
						}
					};
				}
				else {
					thiz._private.schemaMethods["get"+cappedPropName] = function() {
						return this[prop];
					};
					thiz._private.schemaMethods["set"+cappedPropName] = function(value) {
						this[prop] = value;
					};
				}
				propertyDef.init = function(value) {
					delete this[prop];
					this["set"+cappedPropName](value);
				};
			}
		});
		this._private.pkIndex = this._private.indexes[this._private.pk];		
	},
	getPk:function() {
		return this._private.pk;
	},
	normalizeEntity:function(entity) {
		var thiz = this;
		var schema = this._private.schema;
		var methods = this._private.schemaMethods;
		entity._stache = {};
		for (var methodName in methods) {
			entity[methodName] = methods[methodName];
		}
		try {
			for (var prop in schema) {
				var value = entity[prop];
				schema[prop].init.apply(entity, [value]);
			}
		}
		catch(e) {
			this.remove(entity);
			throw(e);
		}
	},
	normalizeEntities:function(entities) {
		var i = entities.length-1;
		do {
			this.normalize(entities[i]);
		} while (i--)
	},
	get:function(criteria) {
		var rows = this.rows(criteria);
		if (rows.length==0) {
			return undefined;
		}
		else {
			return rows[0];
		}
	},
	list:function(criteria) {
		return this.rows(criteria);
	},
	merge:function(arg) {
		if (com.anvesaka.common.isArray(arg)) {
			return this.mergeEntities(arg);
		}
		else {
			return this.mergeEntity(arg);
		}
	},
	mergeEntity:function(entity) {
		var thiz = this;
		if (entity._stache) {
			return entity;
		}
		var pk = this.getPk();
		var id = entity[pk];
		var existingEntity = thiz.get(["ID", id]);
		if (existingEntity) {
			for (var prop in entity) {
				var value = entity[prop];
				var propSpec = thiz._private.schema[prop];
				if (propSpec) {
					if (propSpec.type==="set"||propSpec.type==="list") {
						existingEntity["add"+com.anvesaka.common.capFirst(prop)](value);
					}
					else {
						existingEntity["set"+com.anvesaka.common.capFirst(prop)](value);
					}
				}
				else {
					existingEntity[prop] = value;
				}
			}
			return existingEntity;
		}
		else {
			thiz.normalizeEntity(entity);
			return entity;
		}
	},
	mergeEntities:function(entities) {
		var thiz = this;
		var mergedEntities = [];
		entities.forEach(function(entity) {
			mergedEntities.push(thiz.mergeEntity(entity));
		});
		return mergedEntities;
	},
	remove:function(arg) {
		if (com.anvesaka.common.isArray(arg)) {
			this.removeEntities(arg);
		}
		else {
			this.removeEntity(arg);
		}
	},
	removeEntity:function(entity) {
		var thiz = this;
		for (var prop in thiz._private.indexes) {
			thiz._private.indexes[prop].remove(entity);
		}
	},
	removeEntities:function(entities) {
		var thiz = this;
		entities.forEach(function(entity) {
			thiz.removeEntity(entity);
		});
	},
	/*
	 * Sample criteria:
	 * ["OR", ["ID", 1], ["ID", 2], ["AND", ["RANGE", "lat", 0, 60], ["RANGE", "lon", 40, 70]]]
	 */
	rows:function(criteria) {
		var thiz = this;
		if (criteria.length<2) {
			return [];
		}
		var operation = criteria[0];
		var params = criteria.slice(1, criteria.length);
		if (operation.toUpperCase()==="OR") {
			return com.anvesaka.common.union(params.map(function(operand) {
				return thiz.rows(operand);
			}), this._private.pk);
		}
		else if (operation.toUpperCase()==="AND") {
			return com.anvesaka.common.intersection(params.map(function(operand) {
				return thiz.rows(operand);
			}), this._private.pk);
		}
		else if (operation.toUpperCase()==="ID") {
			return thiz._private.pkIndex.get(params);
		}
		else if (operation.toUpperCase()==="EQ") {
			var prop = params[0];
			var index = this._private.indexes[prop];
			return index.get(params.slice(1, params.length));
		}
		else if (operation.toUpperCase()==="RANGE") {
			var prop = params[0];
			var index = this._private.indexes[prop];
			return index.range(params[1], params[2]);
		}
	}
});

com.anvesaka.common.namespace("com.anvesaka.stache").Stache = Class.extend({
	init:function(options) {
		var thiz = this;
		options = com.anvesaka.common.extend(true, {
			id:"com.anvesaka.stache.DataStore",
			tables:{}
		},
		options);
		this._private = options;
		this._private.tableSpecs = {};
	},
	addTableSpec:function(tableSpec) {
		if (com.anvesaka.common.isDefined(this._private.tableSpecs[tableSpec.name])) {
			com.anvesaka.common.fail("Attempted to add a table specification with a name that is already used.");
		}
		this._private.tableSpecs[tableSpec.name] = com.anvesaka.common.extend(true, tableSpec, {
			db:this
		});
	},
	extendSchema:function(tableName, extensions) {
		this._private.tableSpecs[tableName].schema = com.anvesaka.common.extend(extensions, this._private.tableSpecs[tableName].schema); 
	},
	getTableSpec:function(name) {
		return this._private.tableSpecs[name];
	},	
	activate:function() {
		for (var tableName in this._private.tableSpecs) {
			var tableSpec = this._private.tableSpecs[tableName];
			this._private.tables[tableName] = new com.anvesaka.stache.Table(tableSpec);
		}
		for (var tableName in this._private.tableSpecs) {
			this._private.tables[tableName].processSchema();
		}
	},
	getTable:function(name) {
		return this._private.tables[name];
	},
	dropTable:function(name) {
		delete this._private.tables[name];
	}
});

