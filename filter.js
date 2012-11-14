function(riakObject, keyData, arg) {
	var thing = "if (";
	var entry = eval('(' + riakObject.values[0].data + ')');
	var filter = arg;

	function matchParens(str, openParenIndex) {
		var stack = []; 
		var esc = false;
		for (var i = openParenIndex || 0; i < str.length; i++) {
			var c = str[i];
	
			if (c === '\\') {
				if (!esc)
					esc = true;
				continue;
			} else if (c === '(' && !esc) {
				stack.push(1);
			} else if (c === ')' && !esc) {
				stack.pop();
				if (stack.length === 0)
					return i;
			}
	
			esc = false;
		}
	
		return str.length - 1;
	}

	function match(k, v, type) {
		var r = false;

		if (entry[k] == undefined)
			return r;

		switch (type) {
		case "==":
			if (typeof (entry[k]) !== 'string') {
				entry[k].forEach(function(child) {
					if (child == v)
						r = true;
				});

				return r;
			} else {
				if (entry[k] == v) {
					return true;
				}
			}

			return false;
		case ">=":
			if (Array.isArray(entry[k])) {
				entry[k].forEach(function(child) {
					if (child >= v)
						r = true;
				});

				return r;
			} else {
				if (entry[k] >= v)
					return true;
			}

			return false;
		case "<=":
			if (Array.isArray(entry[k])) {
				entry[k].forEach(function(child) {
					if (child <= v)
						r = true;
				});

				return r;
			} else {
				if (entry[k] <= v)
					return true;
			}

			return false;
		case "sub":
			if (typeof(entry[k]) !== 'string') {
				entry[k].forEach(function(child) {
					if (child.search(v) != -1)
						r = true;
				});

				return r;
			} else {
				if (entry[k].search(v) != -1)
					return true;
			}

			return false;
		}
	}

	function _buildFilterTree(expr) {
		var c;
		var child;
		var clean = false;
		var endParen;
		var esc = false;
		var i = 0;
		var tree = {};
		var split;
		var substrNdx = 0;
		var val = '';
	
		if (expr.length === 0)
			return tree;
		
		if (expr.charAt(0) == '(')
			expr = expr.substring(1, expr.length - 1);
		
		if (expr.charAt(0) === '&') {
			tree.op = 'and';
			expr = expr.substring(1);
		} else if (expr.charAt(0) === '|') {
			tree.op = 'or';
			expr = expr.substring(1);
		} else if (expr.charAt(0) === '!') {
			tree.op = 'not';
			expr = expr.substring(1);
		} else {
			tree.op = 'expr';
		}
	
		if (tree.op != 'expr') {
			tree.children = [];
			
			while (expr.length !== 0) {
				endParen = matchParens(expr);
				if (endParen == expr.length - 1) {
					tree.children[i] = _buildFilterTree(expr);
					expr = '';
				} else {
					child = expr.slice(0, endParen + 1);
					expr = expr.substring(endParen + 1);
					tree.children[i] = _buildFilterTree(child);
				}
				i++;
			}
		} else {
			var operatorStr = '';
			tree.name = '';
			tree.value = '';
			if (expr.indexOf('~=') !== -1) {
				operatorStr = '~=';
				tree.tag = 'approxMatch';
			} else if (expr.indexOf('>=') !== -1) {
				operatorStr = '>=';
				tree.tag = 'greaterOrEqual';
			} else if (expr.indexOf('<=') !== -1) {
				operatorStr = '<=';
				tree.tag = 'lessOrEqual';
			} else if (expr.indexOf(':=') !== -1) {
				operatorStr = ':=';
				tree.tag = 'extensibleMatch';
			} else if (expr.indexOf('=') !== -1) {
				operatorStr = '=';
				tree.tag = 'equalityMatch';
			} else {
				
				throw new Error('invalid filter syntax');
			}
	
			if (operatorStr === '') {
				tree.name = expr;
			} else {
				var splitAry = expr.split(operatorStr);
				tree.name = splitAry.shift();
				tree.value = splitAry.join(operatorStr);
				
				if (tree.tag === 'equalityMatch') {
					if (tree.value === '*') {
						tree.tag = 'present';
					} else {
	
						
						clean = true;
						split = [];
						substrNdx = 0;
						split[substrNdx] = '';
						for (i = 0; i < tree.value.length; i++) {
							c = tree.value[i];
							if (esc) {
								split[substrNdx] += c;
								esc = false;
							} else if (c === '*') {
								split[++substrNdx] = '';
							} else if (c === '\\') {
								esc = true;
							} else {
								split[substrNdx] += c;
							}
						}
	
						if (split.length > 1) {
							tree.tag = 'substrings';
							clean = true;
							
							if (tree.value.indexOf('*') !== 0) {
								tree.initial = split.shift();
							} else {
								split.shift();
							}
							
							if (tree.value.lastIndexOf('*') !== tree.value.length - 1) {
								tree['final'] = split.pop();
							} else {
								split.pop();
							}
							tree.any = split;
						} else {
							tree.value = split[0]; 
						}
					}
				} else if (tree.tag == 'extensibleMatch') {
					split = tree.name.split(':');
					tree.extensible = {
						matchType: split[0],
						value: tree.value
					};
					switch (split.length) {
					case 1:
						break;
					case 2:
						if (split[1].toLowerCase() === 'dn') {
							tree.extensible.dnAttributes = true;
						} else {
							tree.extensible.rule = split[1];
						}
						break;
					case 3:
						tree.extensible.dnAttributes = true;
						tree.extensible.rule = split[2];
						break;
					default:
						throw new Error('Invalid extensible filter');
					}
				}
			}
	
			if (!clean) {
				for (i = 0; i < tree.value.length; i++) {
					c = tree.value[i];
					if (esc) {
						val += c;
						esc = false;
					} else if (c === '\\') {
						esc = true;
					} else {
						val += c;
					}
				}
				tree.value = val;
			}
		}
	
		return tree;
	}


	function buildStatement(tree, type) {
		if (tree === undefined || tree.length === 0)
			return;

		var current = null;

		if (tree.op !== 'expr') {
			switch (tree.op) {
			case 'and':
				current = "&&";
				break;
			case 'or':
				current = "||";
				break;
			case 'not':
				current = "!";
				break;
			default:
				break;
			}

			thing += "(";

			if (current || tree.children.length) {
				tree.children.forEach(function(child) {
					if (current === "!")
						thing += "!(";
					buildStatement(child, current);
					if (current === "!")
						thing += ")";
					else
						thing += " " + current + " ";
				});
			}
			switch(current) {
			case '&&':
				thing += '1)';
				break;
			case '||':
				thing += '0)';
				break;
			case "!":
				thing += ")";
				break;
			}
		} else {
			switch (tree.tag) {
			case 'equalityMatch':
				thing += "match(\"" + tree.name + "\", \"" + tree.value + "\", " + "'==')";
				break;
			case 'greaterOrEqual':
				thing += "match(\"" + tree.name + "\", \"" + tree.value + "\", " + "'>=')";
				break;
			case 'lessOrEqual':
				thing += "match(\"" + tree.name + "\", \"" + tree.value + "\", " + "'<=')";
				break;
			case 'present':
				thing += "entry[\"" + tree.name + "\"]"; 
				break;
			case 'substrings':
				thing += "match(\"" + tree.name + "\", \"" + tree.initial + "\", " + "'sub')";
				break;
			default:
				break;
			}
		}
	}

	t = _buildFilterTree(filter);
	buildStatement(t);

	ret = [];

	thing += ") { ret = [riakObject.key, entry] }";
	eval(thing);

	return ret;
}
